# !/bin/bash
## This script is used to connect to the database using Cloudflare Access. It requires the CF_SSH_ID and CF_SSH_SECRET environment variables to be set with the appropriate values for the service token.
cloudflared access tcp --hostname db-prod.cioospacificlabs.ca --listener localhost:19012 --service-token-id "$CF_SSH_ID" --service-token-secret "$CF_SSH_SECRET"
