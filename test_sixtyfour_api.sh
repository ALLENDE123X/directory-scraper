#!/bin/bash
# Test script for Sixtyfour API endpoint
# Usage: ./test_sixtyfour_api.sh

set -e

source .env

echo "Testing Sixtyfour API Endpoint"
echo "================================"
echo ""

# Test 1: Basic POST request
echo "Test 1: POST request to /api/enrich-lead"
curl -X POST "https://app.sixtyfour.ai/api/enrich-lead" \
  -H "Authorization: Bearer ${SIXTYFOUR_API_KEY}" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-$(date +%s)" \
  -d '{
    "leads": [
      {
        "name": "Test Person",
        "company": "Stanford University",
        "domain": "stanford.edu",
        "email": "test@stanford.edu"
      }
    ]
  }' \
  -v 2>&1 | grep -E "(HTTP|<|>)" | head -20

echo ""
echo "================================"
echo ""

# Test 2: Try alternate paths
echo "Test 2: Trying alternate endpoint paths..."

for path in "/api/v1/enrich-lead" "/api/enrich" "/enrich-lead" "/v1/enrich-lead"; do
  echo "  Testing: $path"
  status=$(curl -X POST "https://app.sixtyfour.ai${path}" \
    -H "Authorization: Bearer ${SIXTYFOUR_API_KEY}" \
    -H "Content-Type: application/json" \
    -w "%{http_code}" \
    -o /dev/null \
    -s)
  echo "    Status: $status"
done

echo ""
echo "================================"
echo ""

# Test 3: Check if GET is accepted
echo "Test 3: Trying GET method on original endpoint"
curl -X GET "https://app.sixtyfour.ai/api/enrich-lead" \
  -H "Authorization: Bearer ${SIXTYFOUR_API_KEY}" \
  -w "\nHTTP Status: %{http_code}\n" \
  -s -o /dev/null

echo ""
echo "================================"
echo ""
echo "If all tests show 405, the endpoint may require:"
echo "  1. Different authentication method"
echo "  2. Different URL structure"
echo "  3. API documentation/support contact"
