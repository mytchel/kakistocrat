struct bst {
  std::string key;
  bst *left, *right;
	posting store;

  bst(std::string &key, uint32_t val);

  ~bst() {
    if (left) delete left;
    if (right) delete right;
  }

  void insert(std::string &key, uint32_t val);

  char* save(char *start, char *ptr_buffer, char *val_buffer);
  char* load(char *start, char *ptr_buffer, char *val_buffer);
};

