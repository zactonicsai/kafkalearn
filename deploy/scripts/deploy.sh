#!/usr/bin/env bash
#
# FreshChain end-to-end deployer.
#
# Reads configuration from environment (or a .env file beside this script),
# deploys the EC2 host via CloudFormation, configures it with Ansible, then
# runs the CLI test suite against the live AWS endpoint.
#
# Required env (put them in deploy/.env):
#   AWS_REGION         e.g. us-east-1
#   KEY_NAME           name of an existing EC2 key pair
#   KEY_PATH           local path to the matching .pem file
# Optional:
#   STACK_NAME         default freshchain
#   INSTANCE_TYPE      default t3.large
#   SSH_CIDR           default your detected public IP /32
#   APP_CIDR           default 0.0.0.0/0
#
# Subcommands:
#   ./deploy.sh up        provision + configure + test   (default)
#   ./deploy.sh test      run tests against existing stack
#   ./deploy.sh info      print stack outputs
#   ./deploy.sh down      delete the CloudFormation stack
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CFN_FILE="$SCRIPT_DIR/cloudformation/freshchain-host.yaml"
ANSIBLE_DIR="$SCRIPT_DIR/ansible"

# ---- load .env if present --------------------------------------------------
if [[ -f "$SCRIPT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  set -a; source "$SCRIPT_DIR/.env"; set +a
fi

STACK_NAME="${STACK_NAME:-freshchain}"
INSTANCE_TYPE="${INSTANCE_TYPE:-t3.large}"
APP_CIDR="${APP_CIDR:-0.0.0.0/0}"
AWS_REGION="${AWS_REGION:-us-east-1}"

c_red(){ printf '\033[91m%s\033[0m\n' "$*"; }
c_grn(){ printf '\033[92m%s\033[0m\n' "$*"; }
c_blu(){ printf '\033[94m%s\033[0m\n' "$*"; }

require(){ command -v "$1" >/dev/null 2>&1 || { c_red "missing dependency: $1"; exit 1; }; }

preflight(){
  require aws; require ansible-playbook; require python3
  : "${KEY_NAME:?set KEY_NAME in deploy/.env}"
  : "${KEY_PATH:?set KEY_PATH in deploy/.env}"
  [[ -f "$KEY_PATH" ]] || { c_red "KEY_PATH not found: $KEY_PATH"; exit 1; }
  aws sts get-caller-identity --region "$AWS_REGION" >/dev/null \
    || { c_red "AWS credentials not configured"; exit 1; }
  # default SSH_CIDR to caller's public IP if unset
  if [[ -z "${SSH_CIDR:-}" ]]; then
    local myip; myip="$(curl -s https://checkip.amazonaws.com || echo '')"
    SSH_CIDR="${myip:+${myip}/32}"; SSH_CIDR="${SSH_CIDR:-0.0.0.0/0}"
  fi
  c_blu "region=$AWS_REGION stack=$STACK_NAME ssh_cidr=$SSH_CIDR"
}

stack_output(){ # $1 = OutputKey
  aws cloudformation describe-stacks --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text 2>/dev/null
}

cmd_up(){
  preflight
  c_blu "==> deploying CloudFormation stack"
  aws cloudformation deploy \
    --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --template-file "$CFN_FILE" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
      KeyName="$KEY_NAME" \
      InstanceType="$INSTANCE_TYPE" \
      SshCidr="$SSH_CIDR" \
      AppCidr="$APP_CIDR"

  local ip; ip="$(stack_output PublicIp)"
  [[ -n "$ip" && "$ip" != "None" ]] || { c_red "could not read PublicIp"; exit 1; }
  c_grn "host public IP: $ip"

  c_blu "==> waiting for SSH"
  for i in $(seq 1 30); do
    if ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
         -i "$KEY_PATH" "ec2-user@$ip" true 2>/dev/null; then break; fi
    sleep 10
  done

  c_blu "==> generating Ansible inventory"
  cat > "$ANSIBLE_DIR/inventory.ini" <<EOF
[freshchain]
$ip ansible_user=ec2-user ansible_ssh_private_key_file=$KEY_PATH

[freshchain:vars]
ansible_ssh_common_args='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
ansible_python_interpreter=/usr/bin/python3
EOF

  c_blu "==> running Ansible"
  ( cd "$ANSIBLE_DIR" && ansible-playbook -i inventory.ini site.yml )

  c_grn "==> deployed. Web UI: http://$ip:8000"
  cmd_test
}

cmd_test(){
  local ip; ip="$(stack_output PublicIp)"
  [[ -n "$ip" && "$ip" != "None" ]] || { c_red "no stack / IP found"; exit 1; }
  c_blu "==> running CLI tests against http://$ip:8000"
  python3 "$REPO_ROOT/tests/test_stack.py" --base "http://$ip:8000"
}

cmd_info(){
  for k in PublicIp WebUrl PublicDnsName SshCommand; do
    printf '%-16s %s\n' "$k" "$(stack_output "$k")"
  done
}

cmd_down(){
  c_blu "==> deleting stack $STACK_NAME"
  aws cloudformation delete-stack --region "$AWS_REGION" --stack-name "$STACK_NAME"
  aws cloudformation wait stack-delete-complete --region "$AWS_REGION" --stack-name "$STACK_NAME"
  c_grn "deleted"
}

case "${1:-up}" in
  up)   cmd_up ;;
  test) cmd_test ;;
  info) cmd_info ;;
  down) cmd_down ;;
  *) echo "usage: $0 {up|test|info|down}"; exit 1 ;;
esac
