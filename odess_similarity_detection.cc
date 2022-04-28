#include "odess_similarity_detection.h"
#include "util/gear_matrix.h"

#include <cassert>
#include <random>
void FeatureIndexTable::Delete(const string &key) {
  SuperFeatures super_features;
  if (GetSuperFeatures(key, &super_features)) {
    ExecuteDelete(key, super_features);
  }
}

void FeatureIndexTable::ExecuteDelete(const string &key,
                                      const SuperFeatures &super_features) {
  for (const super_feature_t &sf : super_features) {
    feature_key_table_[sf].erase(key);
  }
  key_feature_table_.erase(key);
}

void FeatureIndexTable::Put(const string &key, const string &value) {
  SuperFeatures super_features;
  // delete old feature if it exits so we can insert a new one
  Delete(key);

  super_features = feature_generator_.GenerateSuperFeatures(value);
  key_feature_table_[key] = super_features;
  for (const super_feature_t &sf : super_features) {
    feature_key_table_[sf].insert(key);
  }
}

bool FeatureIndexTable::GetSuperFeatures(const string &key,
                                         SuperFeatures *super_features) {
  auto it = key_feature_table_.find(key);
  if (it == key_feature_table_.end()) {
    return false;
  } else {
    *super_features = it->second;
    return true;
  }
}

size_t FeatureIndexTable::CountAllSimilarRecords() const{
  size_t num = 0;
  unordered_set<string> similar_keys;
  for (const auto &it : feature_key_table_) {
    auto keys = it.second;

    // If there are more than one records have the same feature,
    // thoese keys are considered similar
    if (keys.size() > 1) {
      for (const string &key : keys)
        similar_keys.emplace(move(key));
    }
  }
  return similar_keys.size();
}

void FeatureIndexTable::GetSimilarRecordsKeys(const string &key,
                                              vector<string> &similar_keys) {
  SuperFeatures super_features;
  if (!GetSuperFeatures(key, &super_features)) {
    return;
  }

  for (const super_feature_t &sf : super_features) {
    for (string similar_key : feature_key_table_[sf]) {
      if (similar_key != key) {
        similar_keys.emplace_back(move(similar_key));
      }
    }
  }

  for (const string &similar_key : similar_keys) {
    Delete(similar_key);
  }
  ExecuteDelete(key, super_features);
}

FeatureGenerator::FeatureGenerator(feature_t sample_mask, size_t feature_number,
                                   size_t super_feature_number)
    : kSampleRatioMask(sample_mask), kFeatureNumber(feature_number),
      kSuperFeatureNumber(super_feature_number) {
  assert(kFeatureNumber % kSuperFeatureNumber == 0);

  std::random_device rd;
  std::default_random_engine e(rd());
  std::uniform_int_distribution<feature_t> dis(0, UINT64_MAX);

  features_.resize(kFeatureNumber);
  random_transform_args_a_.resize(kFeatureNumber);
  random_transform_args_b_.resize(kFeatureNumber);

  for (size_t i = 0; i < kFeatureNumber; ++i) {
    #ifdef FIX_TRANSFORM_ARGUMENT_TO_KEEP_SAME_SIMILARITY_DETECTION_BETWEEN_TESTS
    random_transform_args_a_[i] = GEARmx[i];
    random_transform_args_b_[i] = GEARmx[kFeatureNumber+i];
    #else
    random_transform_args_a_[i] = dis(e);
    random_transform_args_b_[i] = dis(e);
    #endif
    features_[i] = 0;
  }
}

void FeatureGenerator::OdessResemblanceDetect(const string &value) {
  feature_t hash = 0;
  for (size_t i = 0; i < value.size(); ++i) {
    hash = (hash << 1) + GEARmx[static_cast<uint8_t>(value[i])];
    if (!(hash & kSampleRatioMask)) {
      for (size_t j = 0; j < kFeatureNumber; ++j) {
        feature_t transform_res =
            hash * random_transform_args_a_[j] + random_transform_args_b_[j];
        if (transform_res > features_[j])
          features_[j] = transform_res;
      }
    }
  }
}

SuperFeatures FeatureGenerator::MakeSuperFeatures() {
  if (kSuperFeatureNumber == kFeatureNumber)
    return CopyFeaturesAsSuperFeatures();
  else
    return GroupFeaturesAsSuperFeatures();
}

SuperFeatures FeatureGenerator::CopyFeaturesAsSuperFeatures() {
  SuperFeatures super_features(features_.begin(), features_.end());
  return super_features;
}

// Divede features into groups, then use the group hash as the super feature
SuperFeatures FeatureGenerator::GroupFeaturesAsSuperFeatures() {
  SuperFeatures super_features(kSuperFeatureNumber);
  for (size_t i = 0; i < kSuperFeatureNumber; ++i) {
    size_t group_len = kFeatureNumber / kSuperFeatureNumber;
    super_features[i] = XXH64(&features_[i * group_len],
                              sizeof(feature_t) * group_len, 0x7fcaf1);
  }
  return super_features;
}

void FeatureGenerator::CleanFeatures() {
  for (size_t i = 0; i < kFeatureNumber; ++i) {
    features_[i] = 0;
  }
}

SuperFeatures FeatureGenerator::GenerateSuperFeatures(const string &value) {
  CleanFeatures();
  OdessResemblanceDetect(value);
  return MakeSuperFeatures();
}