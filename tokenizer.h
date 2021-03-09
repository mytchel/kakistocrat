extern "C" {
#include "str.h"
}

namespace tokenizer {

enum token_type {TAG, TAGC, WORD, END};

struct tokenizer {
	size_t index;
	size_t length;
	char *document;

  void init(char *doc, size_t len) {
     index = 0;
     length = len;
     document = doc;
  }

  token_type next(struct str *buffer);
  void skip_tag(char *tag_name_main, struct str *tok_buffer);
  void load_tag_content(struct str *tok_buffer);

  void consume_until(const char *s, struct str *buffer);
};

const size_t tag_name_max_len = 64;
void get_tag_name(char *tag_name, char *token);

const size_t attr_value_max_len = 512;
bool get_tag_attr(char *attr_value, const char *attr_name, char *token);

}

