#!/bin/bash

# Script to create Kubernetes secret for Azure credentials
# Usage: ./create-k8s-secret.sh

echo "Creating Kubernetes secret for Azure credentials..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy .env.example to .env and fill in your Azure credentials"
    exit 1
fi

# Source the .env file
source .env

# Check if required variables are set
if [ -z "$AZURE_TENANT_ID" ] || [ -z "$AZURE_CLIENT_ID" ] || [ -z "$AZURE_CLIENT_SECRET" ]; then
    echo "Error: Missing required Azure credentials in .env file"
    echo "Please ensure AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are set"
    exit 1
fi

# Create the Kubernetes secret
kubectl create secret generic azure-credentials \
    --from-literal=tenant-id="$AZURE_TENANT_ID" \
    --from-literal=client-id="$AZURE_CLIENT_ID" \
    --from-literal=client-secret="$AZURE_CLIENT_SECRET" \
    --dry-run=client -o yaml

echo ""
echo "To apply this secret to your cluster, run:"
echo "kubectl create secret generic azure-credentials \\"
echo "    --from-literal=tenant-id=\"$AZURE_TENANT_ID\" \\"
echo "    --from-literal=client-id=\"$AZURE_CLIENT_ID\" \\"
echo "    --from-literal=client-secret=\"$AZURE_CLIENT_SECRET\""
