default: unittest

tests: unittest

unittest:
	@sh -c "'$(CURDIR)/scripts/tests.sh'"

.PHONY: unittest
