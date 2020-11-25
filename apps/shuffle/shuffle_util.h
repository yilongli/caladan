#pragma once

#include <string>
#include <vector>

#include "net.h"

int get_local_ip(const std::string& ifname);

int parse_netaddr(const char* str, struct netaddr* addr);

std::string netaddr_to_str(struct netaddr addr);

inline void parse_type(const char *s, char **end, int *value)
{
	*value = strtol(s, end, 0);
}

inline void parse_type(const char *s, char **end, int64_t *value)
{
	*value = strtoll(s, end, 0);
}

inline void parse_type(const char *s, char **end, double *value)
{
	*value = strtod(s, end);
}

/**
 * parse() - Parse a value of a particular type from an argument word.
 * @words:     Words of a command being parsed.
 * @i:         Index within words of a word expected to contain an integer
 *             value (may be outside the range of words, in which case an
 *             error message is printed).
 * @value:     The parsed value corresponding to @words[i] is stored here,
 *             if the function completes successfully.
 * @format:    Name of option being parsed (for use in error messages).
 * @type_name: Human-readable name for ValueType (for use in error messages).
 * Return:     Nonzero means success, zero means an error occurred (and a
 *             message was printed).
 */
template<typename ValueType>
int parse(std::vector<std::string> &words, unsigned i, ValueType *value,
		const char *option, const char *type_name)
{
	ValueType num;
	char *end;

	if (i >= words.size()) {
		printf("No value provided for %s\n", option);
		return 0;
	}
	parse_type(words[i].c_str(), &end, &num);
	if (*end != 0) {
		printf("Bad value '%s' for %s; must be %s\n",
				words[i].c_str(), option, type_name);
		return 0;
	}
	*value = num;
	return 1;
}