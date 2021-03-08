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
  char *write(char *start, char *ptr_buffer, char *val_buffer);
};

