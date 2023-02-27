# MapReduce Makefile

# Define variables
PYTHON = python3
CONFIG_FILE = config.json
WORD_COUNT_TEST_DATA = test_data/word_count_input.txt
INVERTED_INDEX_TEST_DATA = test_data/inverted_index_input.txt
WORD_COUNT_OUTPUT = test_data/word_count_output.txt
INVERTED_INDEX_OUTPUT = test_data/inverted_index_output.txt

.PHONY: all test clean

# Default target
all: 

# Test target to run word count and inverted index tests
test: word_count_test inverted_index_test

# Word count test target
word_count_test:
	@echo "Running word count test..."
	@$(PYTHON) run_map_reduce.py $(CONFIG_FILE) word_count $(WORD_COUNT_TEST_DATA) $(WORD_COUNT_OUTPUT)
	@diff $(WORD_COUNT_OUTPUT) test_data/word_count_expected_output.txt
	@echo "Word count test passed!"

# Inverted index test target
inverted_index_test:
	@echo "Running inverted index test..."
	@$(PYTHON) run_map_reduce.py $(CONFIG_FILE) inverted_index $(INVERTED_INDEX_TEST_DATA) $(INVERTED_INDEX_OUTPUT)
	@diff $(INVERTED_INDEX_OUTPUT) test_data/inverted_index_expected_output.txt
	@echo "Inverted index test passed!"

# Clean target to remove output files
clean:
	@echo "Cleaning up output files..."
	@rm -f $(WORD_COUNT_OUTPUT) $(INVERTED_INDEX_OUTPUT)
	@echo "Cleanup complete!"
