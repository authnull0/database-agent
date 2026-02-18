#!/bin/bash

# Exit on any error
set -e
ACTION="$1" # add or delete or modify

if [ -z "$ACTION" ]; then
    echo "Usage: $0 {install|add|delete|modify}"
    exit 1
fi


BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'
BOLD=$(tput bold)
NORMAL=$(tput sgr0)

# Working directory
dir="/opt/authnull-db-agent"
service_binary="db-agent"
service_name="db-agent.service"
service_src="$dir/$service_name"
service_dst="/etc/systemd/system/$service_name"
env_file="$dir/db.env"
DATA_SOURCE_FILE="data-source.yaml"

# Function to print status
print_status() {
    echo -e "${GREEN}[+] $1${NC}"
}

# Function to print error and exit
print_error() {
    echo -e "${RED}[-] ERROR: $1${NC}"
    exit 1
}

#Check Agent running Status 
agent_status() {
    local svc="$1"
    echo "Checking if $svc is already running"
    if pgrep -f "$svc" >/dev/null; then
        echo "Process $svc is running."
        return 0  # success means running
    else 
        echo "Process $svc is not running"
        return 1 # failure means not running 
    fi
}

encrypt_password() {
if [ ! -f "$env_file" ] ; then
    echo "Environment file not found: $env_file"
    exit 1
fi
set -a
source "$env_file"
set +a
if [ -z "$KEY" ] ; then
    echo "KEY not set in $env_file"
    exit 1
fi

  echo -n "$1" | openssl enc -aes-256-cbc -a -salt \
    -pbkdf2 -iter 100000 \
    -pass pass:"$KEY"
}

add_database() {
echo -e "${GREEN}=> Adding new database configuration...${NC}${NORMAL}"
DATA_SOURCE_FILE="data-source.yaml"

# Ensure YAML file exists
if [ ! -f "$DATA_SOURCE_FILE" ]; then
  echo "Creating data source file: $DATA_SOURCE_FILE"
  echo "databases:" > "$DATA_SOURCE_FILE"
fi

# DB type
db_type="postgres"
db_type="$(echo "$db_type" | xargs)"

# Host
read -rp "Enter host: " host
host="$(echo "$host" | xargs)"

# Port (default by type)
db_port="5432"
db_port="$(echo "$db_port" | xargs)"

# Username
read -rp "Enter username: " username
username="$(echo "$username" | xargs)"

# Password (hidden)
read -rsp "Enter password: " password
echo
read -rsp "Confirm password: " confirm_password
echo

if [ "$password" != "$confirm_password" ]; then
  echo "Passwords do not match"
  exit 1
fi

encrypted_password=$(encrypt_password "$password")

# Append to YAML
cat <<EOF >> "$DATA_SOURCE_FILE"
  - host: $host
    type: $db_type
    port: $db_port
    username: $username
    password: ENC($encrypted_password)
EOF
echo "Database '$host' added successfully"
}
delete_database() {
echo -e "${GREEN}=> Deleting database configuration...${NC}${NORMAL}"
# Host
read -rp "Enter host: " host
host="$(echo "$host" | xargs)"
# Verify entry exists
grep -n "^[[:space:]]*- host: $host$" "$DATA_SOURCE_FILE" || {
  echo "Database not found"
  exit 1
}
host="$(echo "$host" | xargs)"
# Remove entry from YAML
sed -i.bak "/host: $host/,+4d" "$DATA_SOURCE_FILE"
echo "Database with host '$host' deleted successfully"
}

modify_database() {
echo -e "${GREEN}=> Modifying database configuration...${NC}${NORMAL}"
# Host
read -rp "Enter host of the database to modify: " host
host="$(echo "$host" | xargs)"
# Verify entry exists
grep -n "^[[:space:]]*- host: $host$" "$DATA_SOURCE_FILE" || {
  echo "Database not found"
  exit 1
}

# New Host
read -rp "Enter new host: " new_host
new_host="$(echo "$new_host" | xargs)"
port="5432"
db_type="postgres"
# New Username
read -rp "Enter new username: " new_username        
new_username="$(echo "$new_username" | xargs)"
# New Password (hidden)
read -rsp "Enter new password: " new_password
echo
read -rsp "Confirm new password: " confirm_password
echo
if [ "$new_password" != "$confirm_password" ]; then
  echo "Passwords do not match"
  exit 1
fi
encrypted_password=$(encrypt_password "$new_password")
# Modify entry in YAML
sed -i.bak "
/^[[:space:]]*- host: $host$/{
  s|^\([[:space:]]*\)- host: .*|\1- host: $new_host|
  n; s|^\([[:space:]]*\)type: .*|\1type: $db_type|
  n; s|^\([[:space:]]*\)port: .*|\1port: $port|
  n; s|^\([[:space:]]*\)username: .*|\1username: $new_username|
  n; s|^\([[:space:]]*\)password: .*|\1password: ENC($encrypted_password)|
}
" "$DATA_SOURCE_FILE"


echo "Database with host '$host' modified successfully"
}


if [ "$ACTION" = "install" ]; then
  echo -e "${GREEN}=> Starting Database Agent Installation...${NC}${NORMAL}"

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run as root or with sudo."
    exit 1
fi

# Stop the existing agent if running
  echo -e "${YELLOW}=> Checking for existing agent service...${NC}${NORMAL}"
  if agent_status "$service_binary"; then
    echo -e "${YELLOW}=> Stopping existing agent service...${NC}${NORMAL}"
    sudo systemctl stop db-agent.service || echo "Service not running, continuing..."
    sudo systemctl disable db-agent.service || echo "Service not enabled, continuing..."
    sudo systemctl daemon-reload
  fi
  if [ ! -d "$dir" ]; then
    echo "Directory does not exist. Creating: $dir"
    mkdir -p "$dir"
    cd "$dir" || exit 1
  else
    echo "Directory already exists: $dir"
    cd "$dir" || exit 1
    echo -e "${YELLOW}=> Cleaning up existing files...${NC}${NORMAL}"    
    rm -rf ./*    
  fi
  
  echo -e "${GREEN}=> Downloading the agent file...${NC}${NORMAL}"
  wget -O db-agent https://github.com/authnull0/database-agent/raw/refs/heads/checkout_postgres/authnull-db-agent

  # Make the agent file executable
  echo -e "${GREEN}\n=> Making script executable...${NC}${NORMAL}"
  chmod +x db-agent
  echo -e "${GREEN}=> Done\n${NC}${NORMAL}"
  # Prompt user to input content for db.env
  echo -e "${GREEN}Please enter the content for the db.env file. End with an empty line or Ctrl+D:${NC}${NORMAL}"

  # Initialize an empty string to store the content
  db_env_content=""

  # Read input line by line
  while IFS= read -r line || [ -n "$line" ]; do
    # Append the line and a newline character to db_env_content
    db_env_content+="$line"$'\n'
  done

  # Remove the trailing newline character
  db_env_content=${db_env_content%$'\n'}

  # Create the db.env file with the provided content
  # sudo echo -n "$db_env_content" > db.env
  echo "$db_env_content" | tee -a db.env

#   # Prompt for host
#   echo -en "${BOLD}${YELLOW}=> Enter host${BLUE}: ${NORMAL}${NC}"
#   read -r host
#   host="$(echo "$host" | xargs)"
#   echo "DB_HOST=$host" | tee -a db.env

#   # Prompt for username
#   echo -en "${BOLD}${YELLOW}=> Enter username${BLUE}: ${NORMAL}${NC}"
#   read -r username
#   username="$(echo "$username" | xargs)"
#   echo "DB_USER=$username" | tee -a db.env
#   # Prompt for password (hidden input)
# echo -en "${BOLD}${YELLOW}=> Enter password${BLUE}: ${NORMAL}${NC}"
# read -s password
# echo

# Remove existing entry if present (optional but recommended)
#sed -i '/^DB_PASSWORD=/d' db.env 2>/dev/null
# # Write password silently
# printf 'DB_PASSWORD=%s\n' "$password" >> db.env

# Remove Windows-style line endings if present
sed -i 's/\r$//' "$env_file"

# Download the service file
  echo -e "${GREEN}=> Downloading the service file...${NC}${NORMAL}"
  wget https://github.com/authnull0/windows-endpoint/raw/refs/heads/postgres-db-agent/agent/linux-build/db-agent.service
  
# Check if /etc/systemd/system is writable
if [ -w /etc/systemd/system ]; then
    echo "/etc/systemd/system is writable"
    sudo mv db-agent.service /etc/systemd/system/
    echo "Service file moved to /etc/systemd/system/db-agent.service"
    sudo chmod 644 /etc/systemd/system/db-agent.service
else
    echo "/etc/systemd/system is NOT writable"
    # Create symlink
    if [ ! -L "$service_dst" ]; then
        sudo ln -s "$service_src" "$service_dst"
        echo "Symlink created: $service_dst → $service_src"
        sudo chmod 644 "$service_dst"
    else
        echo "Symlink already exists"
    fi
fi
echo -e "${GREEN}=> Service file setup completed.${NC}${NORMAL}"
echo " Do you want to input database configurations now? (Y/y to proceed, N/n to skip):"
read -r db_input
if [[ "$db_input" == "Y" || "$db_input" == "y" ]]; then
    add_database
else
    echo "Skipping database configuration input."
fi

# Enable systemd service for the agent
echo -e "${GREEN}=> Enabling and starting the agent service...${NC}${NORMAL}"
sudo systemctl daemon-reload
sudo systemctl start db-agent
sudo systemctl enable db-agent

# # Verify if the agent service is running
# if agent_status "$service_binary"; then
#     echo -e "${GREEN}=> Agent service is running successfully.${NC}${NORMAL}"
# else
#     echo -e "${RED}=> ERROR: Agent service failed to start.${NC}${NORMAL}"
#     exit 1
# fi
cd -

# BEGIN PROXYSQL INSTALLATION
echo -e "${GREEN}=> Starting ProxySQL Installation...${NC}${NORMAL}"
echo "Press Enter( type Y/y) to proceed with ProxySQL installation or N/n to skip if the installation is already done:"
read -r user_input
if [[ "$user_input" == "Y" || "$user_input" == "y" || -z "$user_input" ]]; then
# Update package list
# print_status "Updating package list..."
# apt-get update -y || print_error "Failed to update package list."

print_status "Installing dependencies..."

# apt-get update -y && \
# apt-get install -y \
#     build-essential \
#     automake \
#     cmake \
#     make \
#     git \
#     pkg-config \
#     bzip2 \
#     patch \
#     libtool \
#     uuid-dev \
#     zlib1g-dev \
#     libevent-dev \
#     libjemalloc-dev \
#     libssl-dev \
#     libgnutls28-dev \
#     libicu-dev \
#     nlohmann-json3-dev \
#     default-libmysqlclient-dev \
#     mysql-client-core-8.0 \
#     postgresql \
#     postgresql-contrib \
#     || print_error "Failed to install dependencies."

PACKAGES=(
  build-essential
  automake
  cmake
  make
  git
  pkg-config
  bzip2
  patch
  libtool
  uuid-dev
  zlib1g-dev
  libevent-dev
  libjemalloc-dev
  libssl-dev
  libgnutls28-dev
  libicu-dev
  nlohmann-json3-dev
  default-libmysqlclient-dev
  mysql-client-core-8.0
  postgresql
  postgresql-contrib
)

MISSING_PKGS=()

for pkg in "${PACKAGES[@]}"; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
        MISSING_PKGS+=("$pkg")
    fi
done

if [ "${#MISSING_PKGS[@]}" -eq 0 ]; then
    echo "All dependencies are already installed"
else
    echo "Installing missing packages: ${MISSING_PKGS[*]}"
    apt-get update -y && \
    apt-get install -y "${MISSING_PKGS[@]}" \
        || print_error "Failed to install dependencies."
fi

# check if proxysql service is already running
if agent_status proxysql; then
    print_status "ProxySQL service is already running"
    echo "Stopping existing ProxySQL service..."
    sudo systemctl stop proxysql || print_error "Failed to stop existing ProxySQL service."
    sudo systemctl disable proxysql || print_error "Failed to disable existing ProxySQL service."
else
    print_status "ProxySQL service is not running. Proceeding with configuration."
fi
# Clone ProxySQL repository
print_status "Cloning ProxySQL repository..."
if [ -d "proxysql-v3-alpha" ]; then
    print_status "Directory proxysql-v3-alpha already exists, pulling latest changes..."
    cd proxysql-v3-alpha
    git pull || print_error "Failed to pull latest changes from repository."
else
    git clone https://github.com/authnull0/proxysql-v3-alpha.git || print_error "Failed to clone repository."
    cd proxysql-v3-alpha
fi

# Checkout authsql branch
print_status "Checking out authsql branch..."
git checkout authsql-postgres || print_error "Failed to checkout authsql branch."

# Build ProxySQL
print_status "Cleaning previous build..."
make clean || print_error "Failed to clean previous build."

print_status "Building ProxySQL with $(nproc) jobs..."
make -j$(nproc) || print_error "Failed to build ProxySQL."

print_status "Installing ProxySQL..."
make install || print_error "Failed to install ProxySQL."

# Ensure ProxySQL user and group exist
print_status "Creating proxysql user and group if not exists..."
id -u proxysql &>/dev/null || useradd -r -s /bin/false proxysql
getent group proxysql &>/dev/null || groupadd -r proxysql
usermod -a -G proxysql proxysql || print_error "Failed to configure proxysql user/group."

# Remove existing PID file if it exists
if [ -f "/var/lib/proxysql/proxysql.pid" ]; then
    print_status "Removing existing PID file..."
    rm -f /var/lib/proxysql/proxysql.pid || print_error "Failed to remove PID file."
fi

# Create systemd service file
print_status "Setting up systemd service file..."
if [ -w /usr/lib/systemd/system ]; then
print_status "/usr/lib/systemd/system is writable"
cat > /usr/lib/systemd/system/proxysql.service << EOL
[Unit]
Description=High Performance Advanced Proxy for MySQL
After=network.target

[Service]
Type=forking
ExecStart=/usr/bin/proxysql --idle-threads -c /etc/proxysql.cnf
PIDFile=/var/lib/proxysql/proxysql.pid
User=proxysql
Group=proxysql
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOL
[ $? -eq 0 ] || print_error "Failed to create systemd service file."
else
    echo "/usr/lib/systemd/system is NOT writable"
    echo "Cannot create systemd service file."
    print_error "ProxySQL service file creation failed."
fi

# Set up configuration file and permissions
print_status "Configuring /etc/proxysql.cnf..."
if [ ! -f "/etc/proxysql.cnf" ]; then
    touch /etc/proxysql.cnf || print_error "Failed to create /etc/proxysql.cnf."
fi

if [ ! -f "$env_file" ]; then
    print_error "Environment file not found: $env_file"
fi

# Remove Windows-style line endings if present
sed -i 's/\r$//' "$env_file"

# Export variables from env file
set -a
source "$env_file"
set +a
if [ -z "$ORG_ID" ] || [ -z "$TENANT_ID" ]; then
    print_error "ORG_ID or TENANT_ID not set in $env_file"
fi


# Add authnull section to proxysql.cnf
cat >> /etc/proxysql.cnf << EOL

authnull =
{
    org_id = $ORG_ID
    tenant_id = $TENANT_ID
    api_url = "https://dev.api.authnull.com/authnull0/api/v1/authn/v3/do-authenticationV4"
}
EOL
[ $? -eq 0 ] || print_error "Failed to update /etc/proxysql.cnf."

# Set permissions
print_status "Setting permissions for /etc/proxysql.cnf..."
chown proxysql:proxysql /etc/proxysql.cnf || print_error "Failed to chown /etc/proxysql.cnf."
chmod 644 /etc/proxysql.cnf || print_error "Failed to chmod /etc/proxysql.cnf."

# Create and set permissions for /var/lib/proxysql/
print_status "Setting up /var/lib/proxysql/..."
mkdir -p /var/lib/proxysql/ || print_error "Failed to create /var/lib/proxysql/."
chown proxysql:proxysql /var/lib/proxysql/ || print_error "Failed to chown /var/lib/proxysql/."

# Reload systemd daemon
print_status "Reloading systemd daemon..."
systemctl daemon-reload || print_error "Failed to reload systemd daemon."

# Stop any running ProxySQL instances
print_status "Stopping any running ProxySQL instances..."
killall proxysql 2>/dev/null || true

# Start ProxySQL service
print_status "Starting ProxySQL service..."
systemctl restart proxysql || print_error "Failed to restart proxysql service."

# Enable ProxySQL to start on boot
print_status "Enabling ProxySQL service on boot..."
systemctl enable proxysql || print_error "Failed to enable proxysql service."

print_status "ProxySQL setup completed successfully!"
echo "You can check the status with: systemctl status proxysql"

if agent_status proxysql; then
    print_status "ProxySQL service is running successfully."
else
    print_error "ERROR: ProxySQL service failed to start."
    exit 1
fi
# # Restart db agent once again to ensure connectivity to proxysql
# print_status "Restarting db-agent service to ensure connectivity to ProxySQL..."
# systemctl restart db-agent || print_error "Failed to restart db-agent service."
# if agent_status "$service_binary"; then
#     print_status "db-agent service is running successfully."
# else
#     print_error "ERROR: db-agent service failed to start after ProxySQL installation."
#     exit 1
# fi
elif 
[[ "$user_input" == "N" || "$user_input" == "n" ]]; then
    echo "Skipping ProxySQL installation as per user request."
else
    echo "Invalid input. Skipping ProxySQL installation."
fi 
# print_status "All services are up and running."
print_status "Database Agent Installation and Setup completed successfully."

elif [ "$ACTION" = "add" ]; then
    if [ ! -d "$dir" ]; then
    echo "Directory does not exist. Exiting"
    exit 1
  else
    echo "Directory exists: $dir"
    cd "$dir" 
    fi
    add_database
    # Restart agent to apply changes
    echo -e "${GREEN}=> Restarting agent service to apply new configuration...${NC}${NORMAL}"
    sudo systemctl restart db-agent || print_error "Failed to restart db-agent service."
    echo -e "${GREEN}=> Agent service restarted successfully.${NC}${NORMAL}"

elif [ "$ACTION" = "delete" ]; then
    if [ ! -d "$dir" ]; then
    echo "Directory does not exist. Exiting"
    exit 1
  else
    echo "Directory exists: $dir"
    cd "$dir" 
    fi 
    delete_database
    # Restart agent to apply changes
    echo -e "${GREEN}=> Restarting agent service to apply changes...${NC}${NORMAL}"
    sudo systemctl restart db-agent || print_error "Failed to restart db-agent service."
    echo -e "${GREEN}=> Agent service restarted successfully.${NC}${NORMAL}"

elif [ "$ACTION" = "modify" ]; then
    if [ ! -d "$dir" ]; then
    echo "Directory does not exist. Exiting"
    exit 1
  else
    echo "Directory exists: $dir"
    cd "$dir"  
    fi
    modify_database
    # Restart agent to apply changes
    echo -e "${GREEN}=> Restarting agent service to apply changes...${NC}${NORMAL}"
    sudo systemctl restart db-agent || print_error "Failed to restart db-agent service."
    echo -e "${GREEN}=> Agent service restarted successfully.${NC}${NORMAL}"
    
fi

