namespace tokenizer {

enum token_type {TAG, WORD, END};

struct tokenizer {
	size_t index;
	size_t length;
	char *document;

  void init(char *doc, size_t len) {
     index = 0;
     length = len;
     document = doc;
  }

  token_type next(struct str buffer);
  void skip_tag(char *tag_name_main, struct str tok_buffer);
};

void get_tag_name(char *buf, char *s);
bool should_skip_tag(char *t);

}

