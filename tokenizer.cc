#include <stdio.h>
#include <stdbool.h>
#include <ctype.h>
#include <stdlib.h>

#include <string>
#include <vector>

extern "C" {
#include "str.h"
}

#include "tokenizer.h"
#include "util.h"

namespace tokenizer {

void tokenizer::consume_until(const char * s) {
  size_t i = 0;
  size_t l = strlen(s);

  while (index < length) {
    if (document[index++] == s[i]) {
      i++;
      if (i == l) {
        return;
      }
    } else {
      i = 0;
    }
  }
}

token_type tokenizer::next(struct str *buffer) {
	for (;;) {
		// Whitespace
		while(index < length && isspace(document[index]))
			index++;

		// EOF
		if (index >= length) {
			break;

      // Tags
    } else if (document[index] == '<') {
      size_t i = 0, b = 0;
			char *buf = str_c(buffer);

      index++;
			while (i + index < length) {
        char c = document[index + i++] ;
        if (c == '>')
          break;

        if (i < str_max(buffer))
          buf[b++] = c;
      }

      index += i;

      if (b > 0 && buf[b-1] == '/') {
        buf[b-1] = '\0';
			  str_resize(buffer, b);
			  return TAGC;

      } else {
        buf[b++] = '\0';

        if (util::has_prefix(buf, "script")) {
          consume_until("</script>");

          return next(buffer);

        } else if (util::has_prefix(buf, "style")) {
          consume_until("</style>");

          return next(buffer);
        }

			  str_resize(buffer, b);
			  return TAG;
      }

		// Number
    } else if (isdigit(document[index])) {
			int i = 0, b = 0;
			char *buf = str_c(buffer);
			while (i + index < length && isdigit(document[index + i])) {
        if (i + 1 < str_max(buffer))
				  buf[b++] = document[index + i];

				i++;
			}

			buf[b++] = '\0';
			str_resize(buffer, b);

			index += i;

			return WORD;

		// Word
    } else if (isalpha(document[index])) {
			int i = 0, b = 0;
			char *buf = str_c(buffer);
			while (i + index < length && isalpha(document[index + i])) {
        if (i + 1 < str_max(buffer))
				  buf[b++] = document[index + i];

				i++;
			}

      buf[b++] = '\0';
			str_resize(buffer, b);

			index += i;

			return WORD;

    // Something else we don't want
  	} else {
			index++;
		}
  }

	return END;
}

void tokenizer::skip_tag(char *tag_name_main, struct str *tok_buffer) {
	enum token_type token;

  char tag_name_end[tag_name_max_len];
  tag_name_end[0] = '/';
  strncpy(tag_name_end + 1, tag_name_main, tag_name_max_len);

  do {
    token = next(tok_buffer);

    if (token == TAG) {
      char tag_name[tag_name_max_len];
      get_tag_name(tag_name, str_c(tok_buffer));

      if (strcmp(tag_name_end, tag_name) == 0) {
        break;

      } else if (should_skip_tag(tag_name)) {
        skip_tag(tag_name, tok_buffer);
      }
    }
  } while (token != END);
}

void tokenizer::load_tag_content(struct str *buffer) {
  int i = 0, b = 0;
  char *buf = str_c(buffer);

  while (i + index < length && document[index + i] != '<') {
    if (i + 1 < str_max(buffer))
      buf[b++] = document[index + i];

    i++;
  }

  buf[b++] = '\0';
  str_resize(buffer, b);

  index += i;
}

void get_tag_name(char *buf, char *s) {
  size_t i;
  for (i = 0; i < tag_name_max_len - 1; i++) {
    char c = s[i];
    if (c == '\0' || c == ' ' || c == '\t') break;
    else buf[i] = c;
  }

  buf[i] = '\0';
}

bool get_tag_attr(char *attr_value, const char *attr_name, char *token) {
  size_t i;
  bool in_name = false;
  bool in_value = false;
  bool in_quote = false;
  bool name_match = false;
  bool name_matching = false;
  size_t name_i = 0;
  size_t v = 0;

  for (i = 0; token[i] != '\0'; i++) {
    if (in_value) {
      if (token[i] == '\'' || token[i] == '"') {
        in_quote = !in_quote;

      } else if (name_match) {
        if (v < attr_value_max_len) {
          attr_value[v++] = token[i];
        }
      }

      if (isspace(token[i])) {
        if (in_quote) {
          continue;
        }
      } else {
        continue;
      }
    }

    if (isspace(token[i])) {
      if (name_match) {
        attr_value[v++] = '\0';
        return true;
      }

      in_name = false;
      in_value = false;

      continue;
    }

    if (token[i] == '=') {
      in_name = false;
      in_value = true;

    } else {
      if (!in_name) {
        in_name = true;
        name_i = 0;
        name_match = false;
        name_matching = true;
      }

      if (name_matching && !name_match && attr_name[name_i] == token[i]) {
        name_i++;
        name_matching = true;
        if (attr_name[name_i] == '\0') {
          name_match = true;
        }

      } else {
        name_matching = false;
        name_match = false;
      }
    }
  }

  if (name_match) {
    attr_value[v++] = '\0';
    return true;
  }

  return false;
}

bool should_skip_tag(char *t) {
  return strcmp(t, "head") == 0;
}

}

