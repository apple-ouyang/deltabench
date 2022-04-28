#pragma once
#include "odess_similarity_detection.h"
#include <bits/types/struct_timespec.h>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/filesystem/operations.hpp>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

namespace fs = boost::filesystem;
using namespace fs;
using namespace std;

struct AllData {
  FeatureIndexTable table;
  unordered_map<string, string> key_value;
  unordered_map<string, string> key_compressed_delta;
  unordered_map<string, vector<string>> basekey_similarkeys;
};

struct HumanReadable {
  uintmax_t size_;
  HumanReadable(size_t size = 0) : size_(size){};
  std::string ToString(bool with_byte = true) {
    int magnitude = 0;
    double mantissa = size_;
    while (mantissa >= 1024) {
      mantissa /= 1024.;
      ++magnitude;
    }

    mantissa = ceil(mantissa * 10.) / 10.;

    stringstream ss;
    ss << setprecision(2) << mantissa;
    string res = ss.str();
    res += "BKMGTPE"[magnitude];
    if (magnitude)
      res += 'B';
    if (magnitude && with_byte)
      res += '(' + to_string(size_) + ')';
    return res;
  }

private:
  friend ostream &operator<<(ostream &os, HumanReadable hr) {
    return os << hr.ToString();
  }
};

enum DataSetType : uint8_t {
  kWikipedia,
  kEnronMail,
  kStackOverFlow,
  kStackOverFlowComment
};

const path data_path = "/home/wht/tao-db/test-titan/dataset/DataSet/";
const path wiki_directory = data_path / "wikipedia/article";
const path enron_email_directory = data_path / "enronMail";
const path stack_overflow_directory = data_path / "stackExchange";
const path stack_overflow_comment_file = data_path / "Comments.xml";

string exec(const char *cmd) {
  array<char, 128> buffer;
  string result;
  unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

size_t CountWikipediaHtmls(void) {
  string cmd = "find " + wiki_directory.string() + " -name '*.html' | wc -l";
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

size_t CountEnronEmails(void) {
  string cmd = "find " + enron_email_directory.string() + " | wc -l";
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

size_t CountStackOverFlowXmlFiles() {
  string cmd = "find " + stack_overflow_directory.string() + " | wc -l";
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

size_t CountLinesOfStackOverFlowComment() {
  string cmd = "wc -l " + stack_overflow_comment_file.string();
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

class DataReader {
public:
  DataReader(size_t expected_percentage = 100)
      : expected_percentage_(expected_percentage){};

  void ReadDataPrepare(const DataSetType type) {
    printf("Scaning the number of files that can be Put into the "
           "database...\n");
    printf("Please wait, this may takes a few minutes...\n");
    switch (type) {
    case kWikipedia: {
      to_be_read_ = CountWikipediaHtmls();
      data_directory_ = wiki_directory;
      break;
    }
    case kEnronMail: {
      to_be_read_ = CountEnronEmails();
      data_directory_ = enron_email_directory;
      break;
    }
    case kStackOverFlow: {
      to_be_read_ = CountStackOverFlowXmlFiles();
      data_directory_ = stack_overflow_directory;
      break;
    }
    case kStackOverFlowComment: {
      to_be_read_ = CountLinesOfStackOverFlowComment();
      data_directory_ = stack_overflow_comment_file;
      break;
    }
    default: {
      cerr << "wrong data set type!\n";
      return;
    }
    }
    printf("%zu files can be put into the database\n", to_be_read_);
    printf("Data set path = %s\n", data_directory_.c_str());
  }

  void GetSimilarRecords(const AllData &data) {
    max_similar_records_ = data.table.CountAllSimilarRecords();
  }

  void PrintFinishInfo() {
    printf("\n##################################################\n");
    cout << total_records_ << " records have been put into titan databse!\n";
    cout << put_key_value_size_ << " are the size of keys and values\n";
    printf("%6zu (%.2f%%) is the number of similar records that can be delta "
           "compressed\n",
           max_similar_records_,
           (double)max_similar_records_ / total_records_ * 100);
    printf("##################################################\n\n");
    fflush(stdout);
  }

  void Finish(const AllData &data) {
    GetSimilarRecords(data);
    PrintFinishInfo();
  }

  bool IsFinish() {
    has_been_read_++;
    completion_percentage_ = 100 * has_been_read_ / to_be_read_;
    if (completion_percentage_ - last_completion_ >= 1) {
      printf("\rExpect Put %2zu%%, complete %2zu%%", expected_percentage_,
             completion_percentage_);
      fflush(stdout);
      last_completion_ = completion_percentage_;
    }
    if (completion_percentage_ >= expected_percentage_) {
      printf("\rExpect Put %2zu%%, complete %2zu%%\n", expected_percentage_,
             completion_percentage_);
      return true;
    }
    return false;
  }

  void ParseWikipediaHtml(path path, string &key, string &value) {
    fs::ifstream fin(path);
    stringstream buffer;
    // key: file name
    // key example: Category~Cabarets_in_Paris_7ad0.html
    // value: html file content
    buffer << fin.rdbuf();
    string content = buffer.str();
    string file_name = path.filename().string();
    key = move(file_name);
    value = move(content);
  }

  void ParseEnronMail(path path, string &key, string &value) {
    fs::ifstream fin(path);
    stringstream buffer;
    // key: first line
    // key example: Message-ID: <18782981.1075855378110.JavaMail.evans@thyme>
    // value: remaining email lines
    string first_line;
    getline(fin, first_line);
    buffer << fin.rdbuf();
    string remain_lines = buffer.str();
    key = move(first_line);
    value = move(remain_lines);
  }

  void Put(const string &key, const string &value, AllData &data) {
    data.table.Put(key, value);
    data.key_value[key] = value;
    ++total_records_;
    put_key_value_size_.size_ += key.size() + value.size();
  }

  void ReadFilesUnderDirectoryThenPut(const DataSetType type, AllData &data) {
    for (recursive_directory_iterator f(data_directory_), file_end;
         f != file_end; ++f) {
      if (!is_directory(f->path())) {
        string key, value;
        if (type == kWikipedia) {
          ParseWikipediaHtml(f->path(), key, value);
        } else if (type == kEnronMail) {
          ParseEnronMail(f->path(), key, value);
        } else {
          cerr << "wrong data set type!\n";
          return;
        }
        Put(key, value, data);
      }
      if (IsFinish())
        break;
    }
  }

  // void ReadParseStackOverFlowDataAndPut() {
  //   for (fs::recursive_directory_iterator file(stack_overflow_directory),
  //        file_end;
  //        file != file_end; ++file) {
  //     if (!is_directory(file->path())) {
  //       fs::ifstream fin(file->path());
  //       string line;
  //       while (getline(fin, line)) {
  //         if (line.size() < 3)
  //           continue;
  //         // find record start with <row
  //         size_t found = line.find("<row");
  //         if (found == string::npos) {
  //           continue;
  //         }

  //         size_t start = line.rfind(" Id=");
  //         //<row  ...  Id="12345" ... ></row>
  //         // start:    ^
  //         assert(start != string::npos);

  //         start += 5;
  //         //<row  ... Id="12345" ... ></row>
  //         // start:        ^

  //         size_t end = line.find('"', start);
  //         //<row  ... Id="12345" ... ></row>
  //         // end:               ^

  //         assert(end != string::npos);

  //         // key = Id = "12345"
  //         string key = line.substr(start, end - start);
  //         string &value = line;
  //         Put(key, value);
  //       }
  //     }
  //     // we count files as finish, not records this time.
  //     if (IsFinish())
  //       break;
  //   }
  // }

  // void ReadParseStackOverFlowCommentFileAndPut() {
  //   fs::ifstream fin(stack_overflow_comment_file);
  //   string line;
  //   const int kIdStartPosition = 11;
  //   while (getline(fin, line)) {
  //     // every record has this pattern:
  //     //  <row Id="12345" .../>
  //     // 01234567890
  //     // so we can find the Id through finding the right quotation after the
  //     // left quotation

  //     // some other lines, like start and end of the Comment.xml is not
  //     started
  //     // as "  <row"
  //     if (line.substr(0, 6) != "  <row") {
  //       // cout << line << '\n';
  //       // cout << "doesn't find <row" << "\n\n";
  //       continue;
  //     }

  //     // make sure <row follows the Id=
  //     assert(line.substr(7, 3) == "Id=");

  //     // POS_ID_START=11, means the first number of the Id
  //     size_t pos_id_end = line.find('"', kIdStartPosition);
  //     assert(pos_id_end != string::npos);

  //     string key = line.substr(kIdStartPosition, pos_id_end -
  //     kIdStartPosition); string &value = line; Put(key, value); if
  //     (IsFinish())
  //       break;
  //   }
  // }

  void PutWikipediaData(AllData &data) {
    ReadDataPrepare(kWikipedia);
    ReadFilesUnderDirectoryThenPut(kWikipedia, data);
    Finish(data);
  }

  void PutEnronMailData(AllData &data) {
    ReadDataPrepare(kEnronMail);
    ReadFilesUnderDirectoryThenPut(kEnronMail, data);
    Finish(data);
  }

  // void PutStackOverFlowData(AllData &data) {
  //   ReadDataPrepare(kStackOverFlow);
  //   ReadParseStackOverFlowDataAndPut(data);
  //   Finish();
  // }

  // void PutStackOverFlowCommentData(AllData &data) {
  //   ReadDataPrepare(kStackOverFlowComment);
  //   ReadParseStackOverFlowCommentFileAndPut(data);
  //   Finish();
  // }

  size_t to_be_read_;
  size_t has_been_read_ = 0;
  size_t total_records_ = 0;
  size_t completion_percentage_ = 0;
  size_t last_completion_ = 0;
  // expected_percentage_ range:[1-100]
  // once write data process percentage > expected percentage, write will stop
  size_t expected_percentage_;
  size_t max_similar_records_;
  struct HumanReadable put_key_value_size_;
  path data_directory_;
};