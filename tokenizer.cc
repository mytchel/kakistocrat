#include <stdbool.h>
#include <ctype.h>
#include <stdlib.h>

extern "C" {
#include "x_cocomel/str.h"
}

#include "tokenizer.h"

namespace tokenizer {

token_type tokenizer::next(struct str buffer) {
	for (;;) {
		// Whitespace
		while(index < length && isspace(document[index]))
			index++;

		// EOF
		if (index >= length) {
			break;

      // Ignored tags
    } else if (document[index] == '<') {
      size_t i = 0;
			char *buf = str_c(buffer);

      index++;
			while (i < 256 && i + index < length) {
        if (document[index + i] == '>')
          break;

        buf[i] = document[index + i];
        i++;
      }

      if (i > 0 && buf[i-1] == '/') {
        buf[i-1] = '\0';
			  return TAGC;

      } else {
        buf[i] = '\0';
			  return TAG;
      }

		// Number
    } else if (isdigit(document[index])) {
			int i = 0;
			char *buf = str_c(buffer);
			while (i < 256 && i + index < length && isdigit(document[index + i])) {
				buf[i] = document[index + i];
				i++;
			}
			buf[i] = '\0';
			str_resize(buffer, i);

			index += i;

			return WORD;

		// Word
    } else if (isalpha(document[index])) {
			int i = 0;
			char *buf = str_c(buffer);
			while (i < 256 && i + index < length && isalpha(document[index + i])) {
				buf[i] = tolower(document[index + i]);
				i++;
			}
			buf[i] = '\0';
			str_resize(buffer, i);

			index += i;

			return WORD;

    // Something else we don't want
  	} else {
			index++;
		}
  }

	return END;
}

void tokenizer::skip_tag(char *tag_name_main, struct str tok_buffer) {
	enum token_type token;

  char tag_name_end[33];
  tag_name_end[0] = '/';
  strcpy(tag_name_end + 1, tag_name_main);

  do {
    token = next(tok_buffer);

    if (token == TAG) {
      char tag_name[32];
      get_tag_name(tag_name, str_c(tok_buffer));

      if (strcmp(tag_name_end, tag_name) == 0) {
        break;

      } else if (should_skip_tag(tag_name)) {
        skip_tag(tag_name, tok_buffer);
      }
    }
  } while (token != END);
}

void get_tag_name(char *buf, char *s) {
  size_t i;
  for (i = 0; i < 31; i++) {
    char c = s[i];
    if (c == '\0' || c == ' ' || c == '\t') break;
    else buf[i] = c;
  }

  buf[i] = '\0';
}

bool should_skip_tag(char *t) {
  return strcmp(t, "script") == 0 ||
         strcmp(t, "style") == 0 ||
         strcmp(t, "head") == 0;
}
}

