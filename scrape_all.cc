#include <stdio.h>
#include <unistd.h>

#include <sqlite3.h>

#include <cstdlib>
#include <cstring>

#include "util.h"
#include "scrape.h"

struct data {
  sqlite3 *db;
  int level;
  std::map<std::string, std::vector<std::string>> pending_sites;
};

struct blacklist_data {
  std::string url;
  bool matched;
};

bool is_blacklisted(struct data *data, std::string url) {
  int count = 0;

  char sql[1024];
  snprintf(sql, sizeof(sql),
      "SELECT count(match) FROM blacklist where '%s' like match",
      url.c_str());

  char *err_msg = 0;
  int rc = sqlite3_exec(data->db, sql, 
        [](void *v_data, int argc, char **argv, char **azColName) {
        int *count = (int *) v_data;
        *count = atoi(argv[0]);

        return 0;

      }, &count, &err_msg);

  if (rc != SQLITE_OK ) {
    fprintf(stderr, "SQL error: %s\n", err_msg);

    sqlite3_free(err_msg);
  } 

  return count > 0;
}

void save_indexed(struct data *data, std::map<std::string, std::string> urls) {
  char sql[1024];
  char *err_msg = 0;
  int rc;

  for (auto &p: urls) {
    auto url = p.first;
    auto path = p.second;

    auto host = util::get_host(url);

    snprintf(sql, sizeof(sql),
        "INSERT into links "
        "(host, url, path, internal_ref, external_ref) "
        "values ('%s', '%s', '%s', 1, 0)",
        host.c_str(), url.c_str(), path.c_str());

    rc = sqlite3_exec(data->db, sql, NULL, NULL, &err_msg);

    if (rc != SQLITE_OK ) {
      fprintf(stderr, "SQL error: %s\n", err_msg);

      sqlite3_free(err_msg);
      err_msg = 0;
    } 
  }
}

bool check_mark_url(struct data *data, std::string url) {
  char sql[1024];
  char *err_msg = 0;
  int rc;

  int refs = 0;

  snprintf(sql, sizeof(sql),
      "SELECT external_ref FROM links "
      "WHERE url = '%s'",
      url.c_str());

  rc = sqlite3_exec(data->db, sql, 
        [](void *v_data, int argc, char **argv, char **azColName) {
        int *refs = (int *) v_data;
        *refs = 1 + atoi(argv[0]);

        return 0;
 
        }, &refs, &err_msg);

  if (rc != SQLITE_OK ) {
    fprintf(stderr, "SQL error: %s\n", err_msg);

    sqlite3_free(err_msg);
    err_msg = 0;
    return false;
  }

  if (refs > 0) {
    snprintf(sql, sizeof(sql),
        "UPDATE links set external_ref = %i",
        refs);

    rc = sqlite3_exec(data->db, sql, NULL, NULL, &err_msg);

    if (rc != SQLITE_OK ) {
      fprintf(stderr, "SQL error: %s\n", err_msg);

      sqlite3_free(err_msg);
    } 

    return true;
  
  } else {
    return false;
  }
}

void save_other(struct data *data, std::set<std::string> urls) {
  char sql[1024];
  char *err_msg = 0;
  int rc;

  for (auto &url: urls) {
    if (is_blacklisted(data, url)) {
      printf("blacklisted: %s\n", url.c_str());
      continue;
    }

    if (check_mark_url(data, url)) {
      printf("already indexed: %s\n", url.c_str());
      continue;
    }

    auto host = util::get_host(url);

    // TODO: escape quotes and such things for db calls
    // or find better way to input params
    
    snprintf(sql, sizeof(sql),
        "INSERT into pending (host, url) values ('%s', '%s')",
        host.c_str(), url.c_str());

    rc = sqlite3_exec(data->db, sql, NULL, NULL, &err_msg);

    if (rc != SQLITE_OK ) {
      fprintf(stderr, "SQL error: %s\n", err_msg);

      sqlite3_free(err_msg);
    } 
  }
}

void clear_pending(struct data *data) {
  char *err_msg = 0;
  int rc;

  printf("clear pending urls\n");

  std::string sql = "DELETE FROM pending";

  rc = sqlite3_exec(data->db, sql.c_str(), NULL, NULL, &err_msg);
  if (rc != SQLITE_OK ) {
    fprintf(stderr, "SQL error: %s\n", err_msg);

    sqlite3_free(err_msg);
    exit(1);
  } 
  
  printf("pending urls cleared\n");
} 


void run_round(struct data *data) {
  char *err_msg = 0;
  int rc;

  if (++data->level > 3) {
    printf("reached max level\n");
    return;
  }
    
  printf("run round %i\n", data->level);
 
  data->pending_sites.clear();
  
  std::string sql = "SELECT host, url FROM pending";
  rc = sqlite3_exec(data->db, sql.c_str(), 
        [](void *v_data, int argc, char **argv, char **azColName) {
        struct data *data = (struct data *) v_data;

        std::string host(argv[0]);
        std::string url(argv[1]);

        auto iter = data->pending_sites.find(host);
        if (iter == data->pending_sites.end()) {
          std::vector<std::string> urls;

          urls.push_back(url);

          data->pending_sites.insert(
              std::pair<std::string, std::vector<std::string>>(
                host, urls));
        } else {
          iter->second.push_back(url);
        }

        return 0;

      }, data, &err_msg);

  if (rc != SQLITE_OK ) {
    fprintf(stderr, "SQL error: %s\n", err_msg);

    sqlite3_free(err_msg);
  } 

  clear_pending(data);

  if (data->pending_sites.empty()) {
    printf("no pending sites\n");
    return;
  }
 
  for (auto &p: data->pending_sites) {
    auto host = p.first;
    auto urls = p.second;

    printf("have host %s\n", host.c_str());
    for (auto &u: urls) {
      printf("  %s\n", u.c_str());
    }

    std::map<std::string, std::string> url_indexed;
    std::set<std::string> url_other;

    scrape(10, host, urls, url_indexed, url_other);

    save_indexed(data, url_indexed);
    save_other(data, url_other);
  }
 
  run_round(data);
} 

int main(int argc, char *argv[]) {
    char *err_msg = 0;

    struct data data;

    data.level = 0;
    
    int rc = sqlite3_open("scrape.db", &data.db); if (rc != SQLITE_OK) {
        
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(data.db));
        sqlite3_close(data.db);
        
        return 1;
    }
  
    std::string link_sql = 
        "DROP TABLE IF EXISTS links;"
        "CREATE TABLE links (host text, url text, path text, internal_ref int, external_ref int);";
   
    rc = sqlite3_exec(data.db, link_sql.c_str(), NULL, NULL, &err_msg);
    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error: %s\n", err_msg);

        sqlite3_free(err_msg);
        sqlite3_close(data.db);
        
        return 1;
    } 

    std::string seed_sql = 
        "DROP TABLE IF EXISTS pending;"
        "CREATE TABLE pending (host text, url text);"
        "INSERT INTO pending SELECT host, url FROM seed;";
   
    rc = sqlite3_exec(data.db, seed_sql.c_str(), NULL, NULL, &err_msg);
    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error: %s\n", err_msg);

        sqlite3_free(err_msg);
        sqlite3_close(data.db);
        
        return 1;
    } 

    sleep(1);

    run_round(&data);

    sqlite3_close(data.db);
    
    return 0;
}
    
