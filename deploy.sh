#!/bin/bash

# Deployment script for CHOKE frontend (dev or prod)
# Usage: ./deploy.sh [dev|prod]

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if argument is provided
if [ $# -eq 0 ]; then
    echo -e "${RED}Error: Missing deployment environment argument${NC}"
    echo -e "${YELLOW}Usage: $0 [dev|prod]${NC}"
    echo ""
    echo "Examples:"
    echo "  $0 dev   # Deploy to dev environment"
    echo "  $0 prod  # Deploy to prod environment"
    exit 1
fi

ENVIRONMENT="$1"

# Validate environment argument
if [[ "$ENVIRONMENT" != "dev" && "$ENVIRONMENT" != "prod" ]]; then
    echo -e "${RED}Error: Invalid environment '$ENVIRONMENT'${NC}"
    echo -e "${YELLOW}Valid options: dev or prod${NC}"
    exit 1
fi

# Set variables based on environment
if [ "$ENVIRONMENT" = "dev" ]; then
    GIT_BRANCH="dev"
    CONTAINER_NAME="front-dev"
    PROJECT_NAME="oa-dev"
    echo -e "${BLUE}🚀 Deploying to DEV environment${NC}"
else
    GIT_BRANCH="master"
    CONTAINER_NAME="front-prod"
    PROJECT_NAME="oa-prod"
    echo -e "${BLUE}🚀 Deploying to PROD environment${NC}"
fi
ENV_FILE=".env.prod"
COMPOSE_FILE="docker-compose.prod.frontend.yml"

echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Environment: $ENVIRONMENT"
echo "  Git Branch: $GIT_BRANCH"
echo "  Env File: $ENV_FILE"
echo "  Compose File: $COMPOSE_FILE"
echo "  Container Name: $CONTAINER_NAME"
echo ""

# Step 1: Check if git is available
echo -e "${YELLOW}[1/4]${NC} Checking git..."
if ! command -v git &> /dev/null; then
    echo -e "${RED}Error: git is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ git found${NC}"

# Step 2: Switch git branch and pull
echo -e "${YELLOW}[2/4]${NC} Switching to '$GIT_BRANCH' branch and pulling latest changes..."

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "  Current branch: $CURRENT_BRANCH"

if [ "$CURRENT_BRANCH" != "$GIT_BRANCH" ]; then
    echo "  Switching to $GIT_BRANCH..."
    if ! git checkout "$GIT_BRANCH"; then
        echo -e "${RED}Error: Failed to checkout branch '$GIT_BRANCH'${NC}"
        exit 1
    fi
fi

echo "  Pulling latest changes..."
if ! git pull; then
    echo -e "${RED}Error: Failed to pull latest changes${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Git updated successfully${NC}"

# Step 3: Verify env file exists
echo -e "${YELLOW}[3/5]${NC} Verifying environment files..."
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}Error: Environment file '$ENV_FILE' not found${NC}"
    exit 1
fi
echo "  ✓ $ENV_FILE exists"

if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}Error: Docker compose file '$COMPOSE_FILE' not found${NC}"
    exit 1
fi
echo "  ✓ $COMPOSE_FILE exists"

if [ ! -f "front/Dockerfile" ]; then
    echo -e "${RED}Error: Dockerfile not found at front/Dockerfile${NC}"
    exit 1
fi
echo "  ✓ front/Dockerfile exists"
echo -e "${GREEN}✓ All files verified${NC}"

# Step 4: Build Docker image
echo -e "${YELLOW}[4/5]${NC} Building Docker image..."
echo "  Command: docker compose --project-name '$PROJECT_NAME' --env-file '$ENV_FILE' -f '$COMPOSE_FILE' build front"
echo ""

if docker compose --project-name "$PROJECT_NAME" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" build front; then
    echo -e "${GREEN}✓ Docker image built successfully${NC}"
else
    echo -e "${RED}Error: Failed to build Docker image${NC}"
    exit 1
fi
echo ""

# Step 5: Deploy with docker compose
echo -e "${YELLOW}[5/5]${NC} Deploying with docker compose..."
echo "  Command: docker compose --project-name '$PROJECT_NAME' --env-file '$ENV_FILE' -f '$COMPOSE_FILE' up -d front"
echo ""

if docker compose --project-name "$PROJECT_NAME" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d front; then
    echo ""
    echo -e "${GREEN}✅ Deployment successful!${NC}"
    echo ""
    echo -e "${BLUE}Service Information:${NC}"
    echo "  Environment: $ENVIRONMENT"
    echo "  Project Name: $PROJECT_NAME"
    echo "  Service Name: front"
    echo ""
    
    # Show container status
    CONTAINER_ID=$(docker ps -q --filter "name=${PROJECT_NAME}_front")
    if [ ! -z "$CONTAINER_ID" ]; then
        echo -e "${BLUE}Container Status:${NC}"
        docker ps --filter "name=${PROJECT_NAME}_front" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    fi
    
    echo ""
    echo -e "${YELLOW}Useful commands:${NC}"
    echo "  View logs:     docker compose --project-name '$PROJECT_NAME' -f '$COMPOSE_FILE' logs -f front"
    echo "  Stop service:  docker compose --project-name '$PROJECT_NAME' -f '$COMPOSE_FILE' stop"
    echo "  Restart:       docker compose --project-name '$PROJECT_NAME' -f '$COMPOSE_FILE' restart front"
else
    echo ""
    echo -e "${RED}❌ Deployment failed!${NC}"
    exit 1
fi
