#!/bin/bash
# Configuration validation script for autoloader framework
# This script validates the TSV configuration before running bundle operations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CONFIG_FILE="config/unified_pipeline_config.tsv"
PROFILE="dev"
SHOW_SUMMARY=false
QUIET=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config-file)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --summary)
            SHOW_SUMMARY=true
            shift
            ;;
        --quiet)
            QUIET=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --config-file FILE    Path to configuration TSV file (default: config/unified_pipeline_config.tsv)"
            echo "  --profile PROFILE     Databricks profile (default: dev)"
            echo "  --summary            Show configuration summary"
            echo "  --quiet              Only show errors, no success messages"
            echo "  -h, --help           Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Validate default config"
            echo "  $0 --config-file my_config.tsv       # Validate specific config"
            echo "  $0 --profile prod --summary          # Validate with summary for prod profile"
            echo "  $0 --quiet                           # Quiet validation"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 is not installed or not in PATH${NC}"
    exit 1
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: Configuration file not found: $CONFIG_FILE${NC}"
    exit 1
fi

# Build command
CMD="python3 src/utils/validate_config.py --config-file $CONFIG_FILE"

if [ "$SHOW_SUMMARY" = true ]; then
    CMD="$CMD --summary"
fi

if [ "$QUIET" = true ]; then
    CMD="$CMD --quiet"
fi

# Run validation
echo -e "${BLUE}🔍 Validating autoloader framework configuration...${NC}"
echo -e "${BLUE}   Config file: $CONFIG_FILE${NC}"
echo -e "${BLUE}   Profile: $PROFILE${NC}"
echo ""

# Execute validation
if eval $CMD; then
    if [ "$QUIET" = false ]; then
        echo ""
        echo -e "${GREEN}🎉 Configuration validation successful!${NC}"
        echo -e "${GREEN}   You can now run bundle operations:${NC}"
        echo -e "${GREEN}   - databricks bundle validate --profile $PROFILE${NC}"
        echo -e "${GREEN}   - databricks bundle deploy --profile $PROFILE${NC}"
    fi
    exit 0
else
    echo ""
    echo -e "${RED}❌ Configuration validation failed!${NC}"
    echo -e "${YELLOW}💡 Fix the errors above and run validation again.${NC}"
    echo -e "${YELLOW}   Command: $0 --config-file $CONFIG_FILE${NC}"
    exit 1
fi
