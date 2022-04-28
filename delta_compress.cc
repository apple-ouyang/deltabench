#include "delta_compress.h"
#include "edelta/src/edelta.h"
#include "gdelta/gdelta.h"
#include "gdelta_original/gdelta_original.h"
#include "util/coding.h"
#include "xdelta/xdelta3/xdelta3.h"
#include <cassert>
#include <iostream>
#include <memory>

bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

// Delta compressed delta format:
//
//    +---------------------+------------------+
//    |   original_length   | compressed value |
//    +---------------------+------------------+
//    |       Varint32      |                  |
//    +---------------------+------------------+
bool DeltaCompress(DeltaCompressType type, const string &input,
                   const string &base, string *output) {
  if (type == kNoDeltaCompression) {
    return false;
  }

  if (input.empty() || base.empty())
    return false;

  const size_t kMaxOutLen = input.length() * 2;
  char *buff = new char[kMaxOutLen];
  size_t outlen;
  bool ok;

  uint32_t original_length = input.size();
  PutVarint32(output, original_length);

  switch (type) {
  case kXDelta: {
    int s = xd3_encode_memory((uint8_t *)input.data(), input.length(),
                              (uint8_t *)base.data(), base.length(),
                              (uint8_t *)buff, &outlen, kMaxOutLen, 0);

    ok = (s == 0) && GoodCompressionRatio(outlen, input.size());
    break;
  }
  case kEDelta: {
    if (input.length() > std::numeric_limits<uint32_t>::max()) {
      // Can't compress more than 4GB
      ok = false;
      break;
    }
    EDeltaEncode((uint8_t *)input.data(), (uint32_t)input.length(),
                 (uint8_t *)base.data(), (uint32_t)base.length(),
                 (uint8_t *)buff, (uint32_t *)&outlen);

    ok = GoodCompressionRatio(outlen, input.size());
    break;
  }
  case kGDelta: {
    if (input.length() > std::numeric_limits<uint32_t>::max()) {
      // Can't compress more than 4GB
      ok = false;
      break;
    }
    gencode((uint8_t *)input.data(), (uint32_t)input.length(),
            (uint8_t *)base.data(), (uint32_t)base.length(), (uint8_t **)&buff,
            (uint32_t *)&outlen);

    ok = GoodCompressionRatio(outlen, input.size());
    break;
  }
  case kGdelta_original: {
    if (input.length() > 64 * 1024) {
      // Can't compress more than 64KB
      ok = false;
      break;
    }
    gencode_original((uint8_t *)input.data(), (uint32_t)input.length(),
                     (uint8_t *)base.data(), (uint32_t)base.length(),
                     (uint8_t *)buff, (uint32_t *)&outlen);

    ok = GoodCompressionRatio(outlen, input.size());
    break;
  }
  default: {
  } // Do not recognize this compression type
  }
  output->append(buff, outlen);
  delete[] buff;

  return ok;
}

bool DeltaUncompress(DeltaCompressType type, const string &delta,
                     const string &base, string *output) {
  if (delta.empty() || base.empty())
    return false;
  unsigned char *buff;
  uint32_t original_length;
  string delta_copy(delta);

  if (!GetVarint32(&delta_copy, &original_length)) {
    cerr << "Currupted delta compression";
    return false;
  }

  buff = new unsigned char[original_length];
  size_t output_size = 0;
  assert(type != kNoDeltaCompression);
  bool ok = false;

  switch (type) {
  case kXDelta: {
    int s = xd3_decode_memory(
        (uint8_t *)delta_copy.data(), delta_copy.size(), (uint8_t *)base.data(),
        base.size(), (uint8_t *)buff, &output_size, original_length, 0);

    if (s != 0) {
      cerr << "Corrupted compressed data by kXDelta";
      break;
    }
    ok = true;
    output->assign(buff, buff + output_size);
    break;
  }
  case kEDelta: {
    EDeltaDecode((uint8_t *)delta_copy.data(), delta_copy.size(),
                 (uint8_t *)base.data(), base.size(), (uint8_t *)buff,
                 (uint32_t *)&output_size);
    ok = true;
    output->assign(buff, buff + output_size);
    break;
  }
  case kGDelta: {
    gdecode((uint8_t *)delta_copy.data(), delta_copy.size(),
            (uint8_t *)base.data(), base.size(), (uint8_t **)&buff,
            (uint32_t *)&output_size);
    ok = true;
    output->assign(buff, buff + output_size);
    break;
  }
  case kGdelta_original: {
    gdecode_original((uint8_t *)delta_copy.data(), delta_copy.size(),
                     (uint8_t *)base.data(), base.size(), (uint8_t *)buff,
                     (uint32_t *)&output_size);
    ok = true;
    output->assign(buff, buff + output_size);
    break;
  }
  default:
    cerr << "bad delta compression type";
  }

  if (output_size != original_length) {
    cerr << "output_size=" << output_size
         << " original_length=" << original_length << endl;
    ok = false;
  }

  delete[] buff;
  return ok;
}