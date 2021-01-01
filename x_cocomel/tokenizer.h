#ifndef TOKENIZER_H
#define TOKENIZER_H

#include <stdbool.h>
#include "char.h"
#include "memory.h"
#include "str.h"

enum token_type {TAG, WORD, END};

struct tokenizer
	{
	size_t index;
	size_t length;
	char *document;
	};

static inline void tokenizer_init(struct tokenizer *t, char *doc, size_t len)
	{
	t->index = 0;
	t->length = len;
	t->document = doc;
	}

static inline enum token_type tokenizer_next(struct tokenizer *t, struct str buffer) {
	for (;;) {
		// Whitespace
		while(t->index < t->length && char_isspace(t->document[t->index]))
			t->index++;

		// EOF
		if (t->index >= t->length) {
			break;

      // Ignored tags
    } else if (t->document[t->index] == '<') {
      size_t i = 0;
			char *buf = str_c(buffer);

      t->index++;
			while (i < 256 && i + t->index < t->length) {
        if (t->document[t->index + i] == '>')
          break;

        buf[i] = t->document[t->index + i];
        i++;
      }

      buf[i] = '\0';

			return TAG;

		// Number
    } else if (char_isdigit(t->document[t->index])) {
			int i = 0;
			char *buf = str_c(buffer);
			while (i < 256 && i + t->index < t->length && char_isdigit(t->document[t->index + i]))
				{
				buf[i] = t->document[t->index + i];
				i++;
				}
			buf[i] = '\0';
			str_resize(buffer, i);

			t->index += i;

			return WORD;

		// Word
    } else if (char_isalpha(t->document[t->index])) {
			int i = 0;
			char *buf = str_c(buffer);
			while (i < 256 && i + t->index < t->length && char_isalpha(t->document[t->index + i]))
				{
				buf[i] = char_tolower(t->document[t->index + i]);
				i++;
				}
			buf[i] = '\0';
			str_resize(buffer, i);

			t->index += i;

			return WORD;

    // Something else we don't want
  	} else
			t->index++;
		}

	return END;
}

static inline void tokenizer_get_tag_name(char *buf, char *s) {
  size_t i;
  for (i = 0; i < 31; i++) {
    char c = s[i];
    if (c == '\0' || c == ' ' || c == '\t') break;
    else buf[i] = c;
  }

  buf[i] = '\0';
}

static inline bool tokenizer_should_skip_tag(char *t) {
  return strcmp(t, "script") == 0 ||
         strcmp(t, "style") == 0 ||
         strcmp(t, "head") == 0;
}

static inline void tokenizer_skip_tag(char *tag_name_main, struct tokenizer *tok, struct str tok_buffer) {
	enum token_type token;

  char tag_name_end[33];
  tag_name_end[0] = '/';
  strcpy(tag_name_end + 1, tag_name_main);

  int i = 0;
  do {
    token = tokenizer_next(tok, tok_buffer);

    if (token == TAG) {
      char tag_name[32];
      tokenizer_get_tag_name(tag_name, str_c(tok_buffer));

      if (strcmp(tag_name_end, tag_name) == 0) {
        break;

      } else if (tokenizer_should_skip_tag(tag_name)) {
        tokenizer_skip_tag(tag_name, tok, tok_buffer);
      }
    }
  } while (token != END);
}

#endif

