#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "util/xxhash.h"

using namespace std;

typedef uint64_t feature_t;
typedef unsigned long long super_feature_t;
typedef vector<super_feature_t> SuperFeatures;

// The Mask has X bits of 1's, so the sample rate is 1/(2^X). It means the
// number of sampled chunks to generate feature will be 1/(2^X) of the all
// sliding window chunks.

// 1/(2^9)=1/512
const feature_t k1_512RatioMask = 0x0100400303410010;

// 1/(2^8)=1/256
const feature_t k1_256RatioMask = 0x0100400303410000;

// 1/(2^7)=1/128
const feature_t k1_128RatioMask = 0x0000400303410000;

// 1/(2^2)=1/4
const feature_t k1_4RatioMask = 0x0000000100000001;

#define FIX_TRANSFORM_ARGUMENT_TO_KEEP_SAME_SIMILARITY_DETECTION_BETWEEN_TESTS

class FeatureGenerator {
public:
  static const feature_t kDefaultSampleRatioMask = k1_128RatioMask;
  static const size_t kDefaultFeatureNumber = 12;
  static const size_t kDefaultSuperFeatureNumber = 3;

  /**
   * @description: Detect records similarity. Then we can use delta compression
   * to compress the similar values.
   */
  FeatureGenerator(feature_t sample_mask = kDefaultSampleRatioMask,
                   size_t feature_number = kDefaultFeatureNumber,
                   size_t super_feature_number = kDefaultSuperFeatureNumber);

  SuperFeatures GenerateSuperFeatures(const string &value);

private:
  /**
   * @summary: Use Odess method to calculate the features of a value. The
   * feature is used to detect similarity.
   * @description:  Use Gear hash to calculate the rolling hash of the values.
   * Use content defined method to sample some of chunks hash values. Use
   * different tramsformation methods to sample the hash value as the similarity
   * feature. If two value has a same feature, we consider they are similar.
   * @param &value the value of record.
   */
  void OdessResemblanceDetect(const string &value);

  /**
   * @description: Divide the features into kSuperFeatureNumber groups. Use
   * xxhash on each groups of feature to generate hash value as super feature.
   */
  SuperFeatures MakeSuperFeatures();
  SuperFeatures GroupFeaturesAsSuperFeatures();
  SuperFeatures CopyFeaturesAsSuperFeatures();
  void CleanFeatures();

  vector<feature_t> features_;
  vector<feature_t> random_transform_args_a_;
  vector<feature_t> random_transform_args_b_;

  const feature_t kSampleRatioMask;
  // The super feature are used for similarity detection. The more of super
// features a record have, the bigger feature index table will be.
  const size_t kFeatureNumber;
  const size_t kSuperFeatureNumber;
};

class FeatureIndexTable {
public:
  FeatureIndexTable(){};
  FeatureIndexTable(feature_t sample_mask, size_t feature_number,
                    size_t super_feature_number)
      : feature_generator_(sample_mask, feature_number, super_feature_number){};

  // generate the super features of the value
  // index the key-feature
  void Put(const string &key, const string &value);

  // Delete (key, feature_number of super feature) pair and
  // feature_number of (super feature,key) pairs
  void Delete(const string &key);

  // Use key to find all similar records by searching the key-feature table.
  // After that, remove key from the key-feature table
  void GetSimilarRecordsKeys(const string &key, vector<string> &similar_keys);

  // count all similar records that can be delta compressed
  size_t CountAllSimilarRecords() const;

private:
  unordered_map<super_feature_t, unordered_set<string>> feature_key_table_;
  map<string, SuperFeatures> key_feature_table_;
  FeatureGenerator feature_generator_;

  void ExecuteDelete(const string &key, const SuperFeatures &super_features);

  bool GetSuperFeatures(const string &key, SuperFeatures *super_features);
};