/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_CELLSTOREV5_H
#define HYPERTABLE_CELLSTOREV5_H

#include <map>
#include <string>
#include <vector>

#ifdef _GOOGLE_SPARSE_HASH
#include <google/sparse_hash_set>
#else
#include <ext/hash_set>
#endif

#include "CellStoreBlockIndexArray.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "Common/DynamicBuffer.h"
#include "Common/BloomFilterWithChecksum.h"
#include "Common/BlobHashSet.h"
#include "Common/Mutex.h"

#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/SerializedKey.h"

#include "CellStore.h"
#include "CellStoreTrailerV5.h"
#include "KeyCompressor.h"


/**
 * Forward declarations
 */
namespace Hypertable {
  class BlockCompressionCodec;
  class Client;
  class Protocol;
}

namespace Hypertable {

  class CellStoreV5 : public CellStore {

    class IndexBuilder {
    public:
      IndexBuilder() : m_bigint(false) { }
      void add_entry(KeyCompressorPtr &key_compressor, int64_t offset);
      DynamicBuffer &fixed_buf() { return m_fixed; }
      DynamicBuffer &variable_buf() { return m_variable; }
      bool big_int() { return m_bigint; }
      void chop();
      void release_fixed_buf() { delete [] m_fixed.release(); }
    private:
      DynamicBuffer m_fixed;
      DynamicBuffer m_variable;
      bool m_bigint;
    };

  public:
    CellStoreV5(Filesystem *filesys, Schema *schema=0);
    virtual ~CellStoreV5();

    virtual void create(const char *fname, size_t max_entries, PropertiesPtr &,
                        const TableIdentifier *table_id=0);
    virtual void add(const Key &key, const ByteString value);
    virtual void finalize(TableIdentifier *table_identifier);
    virtual void open(const String &fname, const String &start_row,
                      const String &end_row, int32_t fd, int64_t file_length,
                      CellStoreTrailer *trailer);
    virtual int64_t get_blocksize() { return m_trailer.blocksize; }
    virtual bool may_contain(const void *ptr, size_t len);
    bool may_contain(const String &key) {
      return may_contain(key.data(), key.size());
    }
    virtual bool may_contain(ScanContextPtr &);
    virtual uint64_t disk_usage() { return m_disk_usage; }
    virtual float compression_ratio() { return m_trailer.compression_ratio; }
    virtual const char *get_split_row();
    virtual int64_t get_total_entries() { return m_trailer.total_entries; }
    virtual std::string &get_filename() { return m_filename; }
    virtual int get_file_id() { return m_file_id; }
    virtual CellListScanner *create_scanner(ScanContextPtr &scan_ctx);
    virtual BlockCompressionCodec *create_block_compression_codec();
    virtual KeyDecompressor *create_key_decompressor();
    virtual void display_block_info();
    virtual int64_t end_of_last_block() { return m_trailer.fix_index_offset; }
    virtual size_t bloom_filter_size() { return m_bloom_filter ? m_bloom_filter->size() : 0; }
    virtual int64_t bloom_filter_memory_used() { return m_index_stats.bloom_filter_memory; }
    virtual int64_t block_index_memory_used() { return m_index_stats.block_index_memory; }
    virtual uint64_t purge_indexes();
    virtual bool restricted_range() { return m_restricted_range; }
    virtual const std::vector<String> &get_replaced_files();

    virtual int32_t get_fd() {
      ScopedLock lock(m_mutex);
      return m_fd;
    }

    virtual int32_t reopen_fd() {
      ScopedLock lock(m_mutex);
      if (m_fd != -1)
        m_filesys->close(m_fd);
      m_fd = m_filesys->open(m_filename);
      return m_fd;
    }

    virtual CellStoreTrailer *get_trailer() { return &m_trailer; }

  protected:
    void record_split_row(const SerializedKey key);
    void create_bloom_filter(bool is_approx = false);
    void load_bloom_filter();
    void load_block_index();
    void load_replaced_files();

    typedef BlobHashSet<> BloomFilterItems;

    Mutex                  m_mutex;
    Filesystem            *m_filesys;
    SchemaPtr              m_schema;
    int32_t                m_fd;
    std::string            m_filename;
    CellStoreBlockIndexArray<uint32_t> m_index_map32;
    CellStoreBlockIndexArray<int64_t> m_index_map64;
    bool                   m_64bit_index;
    CellStoreTrailerV5     m_trailer;
    BlockCompressionCodec *m_compressor;
    DynamicBuffer          m_buffer;
    IndexBuilder           m_index_builder;
    DispatchHandlerSynchronizer  m_sync_handler;
    uint32_t               m_outstanding_appends;
    int64_t                m_offset;
    int64_t                m_file_length;
    int64_t                m_disk_usage;
    std::string            m_split_row;
    int                    m_file_id;
    float                  m_uncompressed_data;
    float                  m_compressed_data;
    int64_t                m_uncompressed_blocksize;
    BlockCompressionCodec::Args m_compressor_args;
    size_t                 m_max_entries;

    BloomFilterMode        m_bloom_filter_mode;
    BloomFilterWithChecksum *m_bloom_filter;
    BloomFilterItems      *m_bloom_filter_items;
    int64_t                m_max_approx_items;
    float                  m_bloom_bits_per_item;
    float                  m_filter_false_positive_prob;
    KeyCompressorPtr       m_key_compressor;
    bool                   m_restricted_range;
    int64_t               *m_column_ttl;
    bool                   m_replaced_files_loaded;
  };

  typedef intrusive_ptr<CellStoreV5> CellStoreV5Ptr;

} // namespace Hypertable

#endif // HYPERTABLE_CELLSTOREV5_H
