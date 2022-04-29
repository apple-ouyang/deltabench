#include <cstdint>
#include <string>

using namespace std;

// Similar records can be delta compressed.
// It needs two records to compress, a base record, and a record to be
// compressed to delta. The delta is based on the baes record, using COPY ADD
// method to show the difference between the delta and base
// speed:               edelta > gdelta > xdelta
// compression ratio:   gdelta > xdelta > edelta
// default:             kNoDeltaCompression
// recommend:           kGdelta
enum DeltaCompressType : uint8_t {
  kNoDeltaCompression = 0,
  kXDelta = 1, // traditional delta compression algorithm
  kEDelta = 2, // fastest but also low compression ratio
  kGDelta = 3, // faster and higher compression ratio than Xdelta
  kGdelta_original = 4,
  kGdelta_init = 5,
  kNumberOfDeltaCompression
};

const static string name[kNumberOfDeltaCompression]{
    "no delta compression", "xdelta",     "edelta", "gdelta",
    "gdelta_original",      "gdelta_init"};

inline string ToString(DeltaCompressType type) { return name[type]; }

// Returns true if:
// (1) the compression method is supported in this platform and
// (2) the compression rate is "good enough".
bool DeltaCompress(DeltaCompressType type, const string &input,
                   const string &base, string *output);

// Return true if success
bool DeltaUncompress(DeltaCompressType type, const string &delta,
                     const string &base, string *output);