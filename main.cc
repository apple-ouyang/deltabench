#include "data_reader.h"
#include "delta_compress.h"
#include "odess_similarity_detection.h"
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <iostream>

using namespace std;

void AddElapsedTime(timespec &time, const timespec &start,
                    const timespec &stop) {
  time.tv_nsec += stop.tv_nsec - start.tv_nsec;
  time.tv_sec += stop.tv_sec - start.tv_sec;
  if (time.tv_nsec < 0) {
    time.tv_nsec += 1000000000;
    time.tv_sec--;
  }
  if (time.tv_nsec > 1000000000) {
    time.tv_nsec -= 1000000000;
    time.tv_sec++;
  }
}

double TimespecToSeconds(const timespec &time) {
  return time.tv_sec + time.tv_nsec / 1000000000.;
}

struct Statistics {
  DeltaCompressType type;
  timespec compressed_time{};
  timespec uncompressed_time{};
  HumanReadable original_size{};
  HumanReadable compressed_size{};
  size_t compress_fail = 0;
  size_t compress_success = 0;
  size_t uncompress_fail = 0;

  static void PrintHead() {
    printf(
        "| method           | compress success | compress fail | before "
        "compressed | after  "
        "compressed | compression ratio | compress time | uncompress time |\n");
    printf("| ---------------- | ---------------- | ------------- | "
           "----------------- | "
           "----------------- | ----------------- | ------------- | "
           "--------------- |\n");
  }

  void Print() {
    double ratio = (double)original_size.size_ / compressed_size.size_;
    printf("| %s\t| %zu\t\t| %zu\t\t| %s\t\t| %s\t\t| %.2f\t\t| %.2f\t\t| "
           "%.2f\t\t|\n",
           ToString(type).c_str(), compress_success, compress_fail,
           original_size.ToString(false).c_str(),
           compressed_size.ToString(false).c_str(), ratio,
           TimespecToSeconds(compressed_time),
           TimespecToSeconds(uncompressed_time));
    if (uncompress_fail)
      printf("!!!!!   Uncompress fail %zu times   !!!!!\n", uncompress_fail);
    fflush(stdout);
  }
};

void ScanSimilarRecords(AllData &data) {
  cout << "scaning similar records using Odess similarity detection" << endl;
  for (const auto &it : data.key_value) {
    const string &base_key = it.first;
    vector<string> similar_keys;
    data.table.GetSimilarRecordsKeys(base_key, similar_keys);
    if (!similar_keys.empty())
      data.basekey_similarkeys[base_key] = move(similar_keys);
  }
}

void CleanCompressedDeltas(AllData &data) { data.key_compressed_delta.clear(); }

void StartDeltaCompress(AllData &data, const DeltaCompressType type,
                        Statistics &stat) {
  for (auto &it : data.basekey_similarkeys) {
    const string &base_key = it.first;
    const vector<string> &similar_keys = it.second;
    const string &base = data.key_value[base_key];

    vector<string> compress_success_keys;
    for (const string &similar_key : similar_keys) {
      string delta;
      const string &input = data.key_value[similar_key];

      struct timespec start, stop;
      clock_gettime(CLOCK_MONOTONIC, &start);
      assert(!input.empty() && !base.empty());
      bool ok = DeltaCompress(type, input, base, &delta);
      clock_gettime(CLOCK_MONOTONIC, &stop);
      AddElapsedTime(stat.compressed_time, start, stop);
      if (!ok) {
        stat.compress_fail++;
      } else {
        stat.compress_success++;
        stat.original_size.size_ += input.size();
        stat.compressed_size.size_ += delta.size();
        compress_success_keys.push_back(move(similar_key));
      }
      data.key_compressed_delta[similar_key] = move(delta);
    }
    vector<string> &delta_keys = it.second;
    delta_keys = move(compress_success_keys);
  }
}

void StartDeltaUncompress(AllData &data, const DeltaCompressType type,
                          Statistics &stat) {
  for (const auto &it : data.basekey_similarkeys) {
    const string &base_key = it.first;
    const vector<string> &delta_keys = it.second;
    const string &base = data.key_value[base_key];
    for (const string &delta_key : delta_keys) {
      string output;
      const string &delta = data.key_compressed_delta[delta_key];

      struct timespec start, stop;
      clock_gettime(CLOCK_MONOTONIC, &start);
      assert(!delta.empty() && !base.empty());
      bool ok = DeltaUncompress(type, delta, base, &output);
      clock_gettime(CLOCK_MONOTONIC, &stop);
      AddElapsedTime(stat.uncompressed_time, start, stop);

      const string &original = data.key_value[delta_key];
      if (!ok) {
        ++stat.uncompress_fail;
      }
    }
  }
}

void TestDataSet(DataSetType dataset) {
  DataReader data_reader;
  AllData *new_data = new AllData();
  AllData &data = *new_data;

  switch (dataset) {
  case kWikipedia: {
    data_reader.PutWikipediaData(data);
    break;
  }
  case kEnronMail: {
    data_reader.PutEnronMailData(data);
    break;
  }
  default: {
    cerr << "dataset type not support" << endl;
    return;
  }
  }

  ScanSimilarRecords(data);
  cout << "start delta compress" << endl;
  Statistics::PrintHead();
  for (uint8_t i = kXDelta; i < kMaxDeltaCompression; ++i) {
    DeltaCompressType type = (DeltaCompressType)i;
    Statistics stat;
    stat.type = type;

    CleanCompressedDeltas(data);
    StartDeltaCompress(data, type, stat);
    StartDeltaUncompress(data, type, stat);

    stat.Print();
  }
  delete new_data;
}

int main() {
  for (uint8_t dataset = kWikipedia; dataset <= kEnronMail; ++dataset)
    TestDataSet((DataSetType)dataset);
  return 0;
}