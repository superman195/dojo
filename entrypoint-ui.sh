#!/bin/sh
set -e

echo "Starting Dojo UI container..."
echo "DISABLE_CSP: $DISABLE_CSP"

# Remove CSP restrictions if DISABLE_CSP is true
if [ "$DISABLE_CSP" = "true" ]; then
  echo "Disabling Content Security Policy..."
  find /app/.next -type f -name "*.html" -exec sed -i 's/<meta[^>]*Content-Security-Policy[^>]*>//g' {} \;
fi


echo "Starting Next.js..."
exec npm start
