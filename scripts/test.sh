#!/usr/bin/env bash
# SoHoLINK Test Runner
# Runs tests with various configurations and outputs results

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
TEST_TIMEOUT=${TEST_TIMEOUT:-120s}
SHORT_MODE=${SHORT_MODE:-false}
RACE_DETECTION=${RACE_DETECTION:-true}
COVERAGE=${COVERAGE:-true}

# Parse arguments
while [[ $# -gt 0 ]]; do
	case $1 in
		--short)
			SHORT_MODE=true
			shift
			;;
		--no-race)
			RACE_DETECTION=false
			shift
			;;
		--no-coverage)
			COVERAGE=false
			shift
			;;
		--help)
			echo "Usage: $0 [OPTIONS]"
			echo ""
			echo "Options:"
			echo "  --short        Run tests in short mode (skip slow tests)"
			echo "  --no-race      Disable race detection"
			echo "  --no-coverage  Disable coverage report"
			echo "  --help         Show this help message"
			echo ""
			echo "Environment Variables:"
			echo "  TEST_TIMEOUT   Test timeout (default: 120s)"
			echo "  SHORT_MODE     Run in short mode (default: false)"
			echo "  RACE_DETECTION Enable race detection (default: true)"
			echo "  COVERAGE       Enable coverage (default: true)"
			exit 0
			;;
		*)
			echo "Unknown option: $1"
			echo "Run '$0 --help' for usage information"
			exit 1
			;;
	esac
done

# Header
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  SoHoLINK Test Suite${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo ""

# Build flags
TEST_FLAGS=(-v -timeout "$TEST_TIMEOUT")
if [ "$SHORT_MODE" = true ]; then
	TEST_FLAGS+=(-short)
	echo -e "${YELLOW}Mode: Short (skipping slow tests)${NC}"
else
	echo -e "${YELLOW}Mode: Full${NC}"
fi

if [ "$RACE_DETECTION" = true ]; then
	TEST_FLAGS+=(-race)
	echo -e "${YELLOW}Race detection: Enabled${NC}"
else
	echo -e "${YELLOW}Race detection: Disabled${NC}"
fi

if [ "$COVERAGE" = true ]; then
	TEST_FLAGS+=(-coverprofile=coverage.out)
	echo -e "${YELLOW}Coverage: Enabled${NC}"
else
	echo -e "${YELLOW}Coverage: Disabled${NC}"
fi

echo ""
echo -e "${GREEN}───────────────────────────────────────────${NC}"
echo -e "${GREEN}Running tests...${NC}"
echo -e "${GREEN}───────────────────────────────────────────${NC}"
echo ""

# Run tests
if go test "${TEST_FLAGS[@]}" ./internal/...; then
	echo ""
	echo -e "${GREEN}✓ All tests passed!${NC}"
	EXIT_CODE=0
else
	echo ""
	echo -e "${RED}✗ Some tests failed!${NC}"
	EXIT_CODE=1
fi

# Generate coverage report if enabled
if [ "$COVERAGE" = true ] && [ -f coverage.out ]; then
	echo ""
	echo -e "${GREEN}───────────────────────────────────────────${NC}"
	echo -e "${GREEN}Coverage Report${NC}"
	echo -e "${GREEN}───────────────────────────────────────────${NC}"
	echo ""

	# Generate HTML report
	go tool cover -html=coverage.out -o coverage.html
	echo -e "HTML report: ${YELLOW}coverage.html${NC}"

	# Show coverage summary
	echo ""
	go tool cover -func=coverage.out | tail -1
	echo ""

	# Coverage by package
	echo -e "${YELLOW}Coverage by package:${NC}"
	go tool cover -func=coverage.out | grep -v "total:" | awk '{print $1, $NF}' | column -t
fi

# Test summary
echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}Test Summary${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"

if [ $EXIT_CODE -eq 0 ]; then
	echo -e "${GREEN}Status: PASSED ✓${NC}"
else
	echo -e "${RED}Status: FAILED ✗${NC}"
fi

if [ "$COVERAGE" = true ] && [ -f coverage.out ]; then
	TOTAL_COV=$(go tool cover -func=coverage.out | grep total: | awk '{print $NF}')
	echo -e "Coverage: ${YELLOW}${TOTAL_COV}${NC}"
fi

echo ""

exit $EXIT_CODE
