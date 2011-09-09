/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#include "Common/Compat.h"
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <iterator>
#include <vector>

#include "Common/Error.h"
#include "Common/md5.h"

#include "AccessGroup.h"
#include "CellCache.h"
#include "CellCacheScanner.h"
#include "CellStoreFactory.h"
#include "CellStoreReleaseCallback.h"
#include "CellStoreV5.h"
#include "Global.h"
#include "MaintenanceFlag.h"
#include "MergeScannerAccessGroup.h"
#include "MetadataNormal.h"
#include "MetadataRoot.h"
#include "Config.h"

using namespace Hypertable;


AccessGroup::AccessGroup(const TableIdentifier *identifier,
    SchemaPtr &schema, Schema::AccessGroup *ag, const RangeSpec *range)
  : m_outstanding_scanner_count(0), m_identifier(*identifier), m_schema(schema),
    m_name(ag->name), m_next_cs_id(0), m_disk_usage(0),
    m_compression_ratio(1.0), m_earliest_cached_revision(TIMESTAMP_MAX),
    m_earliest_cached_revision_saved(TIMESTAMP_MAX),
    m_latest_stored_revision(TIMESTAMP_MIN), m_collisions(0),
    m_file_tracker(identifier, schema, range, ag->name), m_is_root(false),
    m_recovering(false), m_needs_merging(false) {

  m_table_name = m_identifier.id;
  m_start_row = range->start_row;
  m_end_row = range->end_row;
  m_range_name = m_table_name + "[" + m_start_row + ".." + m_end_row + "]";
  m_full_name = m_range_name + "(" + m_name + ")";

  range_dir_initialize();

  foreach(Schema::ColumnFamily *cf, ag->columns)
    m_column_families.insert(cf->id);

  m_garbage_tracker.set_schema(schema, ag);

  m_is_root = (m_identifier.is_metadata() && *range->start_row == 0
               && !strcmp(range->end_row, Key::END_ROOT_ROW));
  m_in_memory = ag->in_memory;

  m_cellstore_props = new Properties();
  m_cellstore_props->set("compressor", ag->compressor.size() ?
      ag->compressor : schema->get_compressor());
  m_cellstore_props->set("blocksize", ag->blocksize);
  if (ag->replication != -1)
    m_cellstore_props->set("replication", (int32_t)ag->replication);

  if (ag->bloom_filter.size())
    Schema::parse_bloom_filter(ag->bloom_filter, m_cellstore_props);
  else {
    assert(Config::properties); // requires Config::init* first
    Schema::parse_bloom_filter(Config::get_str("Hypertable.RangeServer"
        ".CellStore.DefaultBloomFilter"), m_cellstore_props);
  }
  m_bloom_filter_disabled = BLOOM_FILTER_DISABLED ==
      m_cellstore_props->get<BloomFilterMode>("bloom-filter-mode");
}


/**
 * Currently supports only adding and deleting column families
 * from AccessGroup. Changing other attributes of existing
 * AccessGroup is not supported.
 * Schema is only updated if the new schema has a more recent generation
 * number than the existing schema.
 */
void AccessGroup::update_schema(SchemaPtr &schema,
                                Schema::AccessGroup *ag) {
  ScopedLock lock(m_mutex);
  std::set<uint8_t>::iterator iter;

  m_garbage_tracker.set_schema(schema, ag);

  if (schema->get_generation() > m_schema->get_generation()) {
    foreach(Schema::ColumnFamily *cf, ag->columns) {
      if((iter = m_column_families.find(cf->id)) == m_column_families.end()) {
        // Add new column families
        if(cf->deleted == false ) {
          m_column_families.insert(cf->id);
        }
      }
      else {
        // Delete existing cfs
        if (cf->deleted == true) {
          m_column_families.erase(iter);
        }
        // TODO: In future other types of updates
        // such as alter table modify etc will go in here
      }
    }

    // Update schema ptr
    m_schema = schema;
  }
}

/**
 * This should be called with the CellCache locked Also, at the end of
 * compaction processing, when m_cell_cache gets reset to a new value, the
 * CellCache should be locked as well.
 */
void AccessGroup::add(const Key &key, const ByteString value) {
  if (!m_cell_cache) {
    m_cell_cache = new CellCache();
  }
  if (key.revision > m_latest_stored_revision || Global::ignore_clock_skew_errors) {
    if (key.revision < m_earliest_cached_revision)
      m_earliest_cached_revision = key.revision;
    if (m_schema->column_is_counter(key.column_family_code))
      return m_cell_cache->add_counter(key, value);
    else
      return m_cell_cache->add(key, value);
  }
  else if (!m_recovering) {
    HT_ERROR("Revision (clock) skew detected! May result in data loss.");
    //HT_DEBUG_OUT << "skew detected for key=" << key.row
    //    << " key.revision=" << key.revision << " latest_stored_revision="
    //    << m_latest_stored_revision << " earliest_cached_revision="
    //    << m_earliest_cached_revision << HT_END;
    if (m_schema->column_is_counter(key.column_family_code))
      return m_cell_cache->add_counter(key, value);
    else
      return m_cell_cache->add(key, value);
  }
  else if (m_in_memory) {
    if (m_schema->column_is_counter(key.column_family_code))
      return m_cell_cache->add_counter(key, value);
    else
      return m_cell_cache->add(key, value);
  }
}


CellListScanner *AccessGroup::create_scanner(ScanContextPtr &scan_context) {
  bool all = scan_context->spec ? scan_context->spec->return_deletes : false;
  MergeScanner *scanner = new MergeScannerAccessGroup(scan_context, all);

  CellStoreReleaseCallback callback(this);

  {
    ScopedLock lock(m_outstanding_scanner_mutex);
    m_outstanding_scanner_count++;
  }

  try {
    ScopedLock lock(m_mutex);
    uint64_t initial_bytes_read;

    if (m_cell_cache)
      scanner->add_scanner(m_cell_cache->create_scanner(scan_context));

    if (m_immutable_cache)
      scanner->add_scanner(m_immutable_cache->create_scanner(scan_context));

    if (!m_in_memory) {
      bool bloom_filter_disabled;

      for (size_t i=0; i<m_stores.size(); ++i) {

        if (scan_context->time_interval.first > m_stores[i].timestamp_max ||
            scan_context->time_interval.second < m_stores[i].timestamp_min)
          continue;

        bloom_filter_disabled = boost::any_cast<uint8_t>(m_stores[i].cs->get_trailer()->get("bloom_filter_mode")) == BLOOM_FILTER_DISABLED;

        initial_bytes_read = m_stores[i].cs->bytes_read();

        // Query bloomfilter only if it is enabled and a start row has been specified
        // (ie query is not something like select bar from foo;)
        if (bloom_filter_disabled ||
            !scan_context->single_row ||
            scan_context->start_row == "") {
          if (m_stores[i].shadow_cache) {
            scanner->add_scanner(m_stores[i].shadow_cache->create_scanner(scan_context));
            m_stores[i].shadow_cache_hits++;
          }
          else
            scanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
          callback.add_file(m_stores[i].cs->get_filename());
        }
        else {
          m_stores[i].bloom_filter_accesses++;
          if (m_stores[i].cs->may_contain(scan_context)) {
            m_stores[i].bloom_filter_maybes++;
            if (m_stores[i].shadow_cache) {
              scanner->add_scanner(m_stores[i].shadow_cache->create_scanner(scan_context));
              m_stores[i].shadow_cache_hits++;
            }
            else
              scanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
            callback.add_file(m_stores[i].cs->get_filename());
          }
        }

        if (m_stores[i].cs->bytes_read() > initial_bytes_read)
          scanner->add_disk_read(m_stores[i].cs->bytes_read() - initial_bytes_read);

      }
    }
  }
  catch (Exception &e) {
    ScopedLock lock(m_outstanding_scanner_mutex);
    m_outstanding_scanner_count--;
    delete scanner;
    HT_THROW2F(e.code(), e, "Problem creating scanner on access group %s",
               m_full_name.c_str());
  }

  m_file_tracker.add_references( callback.get_file_vector() );
  scanner->install_release_callback(callback);

  return scanner;
}

bool AccessGroup::include_in_scan(ScanContextPtr &scan_context) {
  ScopedLock lock(m_mutex);
  for (std::set<uint8_t>::iterator iter = m_column_families.begin();
       iter != m_column_families.end(); ++iter) {
    if (scan_context->family_mask[*iter])
      return true;
  }
  return false;
}

const char *AccessGroup::get_split_row() {
  std::vector<String> split_rows;
  get_split_rows(split_rows, true);
  if (split_rows.size() > 0) {
    sort(split_rows.begin(), split_rows.end());
    return (split_rows[split_rows.size()/2]).c_str();
  }
  return "";
}

void
AccessGroup::get_split_rows(std::vector<String> &split_rows,
                            bool include_cache) {
  ScopedLock lock(m_mutex);
  const char *row;

  for (size_t i=0; i<m_stores.size(); i++) {
    row = m_stores[i].cs->get_split_row();
    if (row) {
      split_rows.push_back(row);
    }
  }
  if (include_cache) {
    if (m_cell_cache) {
      if (m_immutable_cache &&
          m_immutable_cache->size() > m_cell_cache->size())
        m_immutable_cache->get_split_rows(split_rows);
      else
        m_cell_cache->get_split_rows(split_rows);
    }
    else if (m_immutable_cache)
      m_immutable_cache->get_split_rows(split_rows);
  }
}

void AccessGroup::get_cached_rows(std::vector<String> &rows) {
  ScopedLock lock(m_mutex);
  if (m_cell_cache) {
    if (m_immutable_cache &&
        m_immutable_cache->size() > m_cell_cache->size())
      m_immutable_cache->get_rows(rows);
    else
      m_cell_cache->get_rows(rows);
  }
  else if (m_immutable_cache)
    m_immutable_cache->get_rows(rows);
}


uint64_t AccessGroup::disk_usage() {
  ScopedLock lock(m_mutex);
  uint64_t usage;
  uint64_t du = (m_in_memory) ? 0 : m_disk_usage;
  uint64_t mu = m_cell_cache ? m_cell_cache->memory_used() : 0;
  if (m_immutable_cache)
    mu += m_immutable_cache->memory_used();
  usage = du + (uint64_t)(m_compression_ratio * (float)mu);
  return usage;
}

uint64_t AccessGroup::memory_usage() {
  ScopedLock lock(m_mutex);
  uint64_t mu = m_cell_cache ? m_cell_cache->memory_used() : 0;
  if (m_immutable_cache)
    mu += m_immutable_cache->memory_used();

  if (mu < 0)
    HT_WARN_OUT << "[Issue 339] Memory usage for " << m_full_name << "=" << mu
                << HT_END;
  return mu;
}

void AccessGroup::space_usage(int64_t *memp, int64_t *diskp) {
  ScopedLock lock(m_mutex);
  *memp = m_cell_cache ? m_cell_cache->memory_used() : 0;
  if (m_immutable_cache)
    *memp += m_immutable_cache->memory_used();
  *diskp = (m_in_memory) ? 0 : m_disk_usage;
  *diskp += (int64_t)(m_compression_ratio * (float)*memp);
}


uint64_t AccessGroup::purge_memory(MaintenanceFlag::Map &subtask_map) {
  ScopedLock lock(m_outstanding_scanner_mutex);
  uint64_t memory_purged = 0;
  int flags;

  {
    ScopedLock lock(m_mutex);
    for (size_t i=0; i<m_stores.size(); i++) {
      flags = subtask_map.flags(m_stores[i].cs.get());
      if (MaintenanceFlag::purge_shadow_cache(flags) &&
	  m_stores[i].shadow_cache) {
	memory_purged += m_stores[i].shadow_cache->memory_allocated();
	m_stores[i].shadow_cache = 0;
      }
      if (m_outstanding_scanner_count == 0 &&
	  MaintenanceFlag::purge_cellstore(flags))
	memory_purged += m_stores[i].cs->purge_indexes();
    }
  }

  return memory_purged;
}

AccessGroup::MaintenanceData *AccessGroup::get_maintenance_data(ByteArena &arena, time_t now) {
  ScopedLock lock(m_mutex);
  MaintenanceData *mdata = (MaintenanceData *)arena.alloc(sizeof(MaintenanceData));
  int64_t key_bytes = 0, value_bytes = 0;
  size_t cell_count = 0;
  int64_t mu = 0;

  memset(mdata, 0, sizeof(MaintenanceData));
  mdata->ag = this;

  if (m_earliest_cached_revision_saved != TIMESTAMP_MAX)
    mdata->earliest_cached_revision = m_earliest_cached_revision_saved;
  else
    mdata->earliest_cached_revision = m_earliest_cached_revision;

  mdata->latest_stored_revision = m_latest_stored_revision;

  if (m_cell_cache) {
    m_cell_cache->get_counts(&cell_count, &key_bytes, &value_bytes);
    mu = m_cell_cache->memory_used();
    mdata->mem_allocated = m_cell_cache->memory_allocated();
    mdata->mem_used = m_cell_cache->memory_used();
    mdata->deletes = m_cell_cache->get_delete_count();
  }

  mdata->cached_items = cell_count;
  mdata->key_bytes = key_bytes;
  mdata->value_bytes = value_bytes;

  if (m_immutable_cache) {
    mu += m_immutable_cache->memory_used();
    m_immutable_cache->get_counts(&cell_count, &key_bytes, &value_bytes);
    mdata->immutable_items = cell_count;
    mdata->key_bytes += key_bytes;
    mdata->value_bytes += value_bytes;
    mdata->mem_allocated += m_immutable_cache->memory_allocated();
  }
  else
    mdata->immutable_items = 0;

  mdata->compression_ratio = (m_compression_ratio == 0.0) ? 1.0 : m_compression_ratio;
  mdata->cell_count = mdata->cached_items + mdata->immutable_items;

  mdata->disk_used = m_disk_usage;
  int64_t du = m_in_memory ? 0 : m_disk_usage;
  mdata->disk_estimate = du + (int64_t)(m_compression_ratio * (float)mu);
  mdata->outstanding_scanners = m_outstanding_scanner_count;
  mdata->in_memory = m_in_memory;

  CellStoreMaintenanceData **tailp = 0;
  mdata->csdata = 0;
  for (size_t i=0; i<m_stores.size(); i++) {
    if (mdata->csdata == 0) {
      mdata->csdata = (CellStoreMaintenanceData *)arena.alloc(sizeof(CellStoreMaintenanceData));
      mdata->csdata->cs = m_stores[i].cs.get();
      tailp = &mdata->csdata;
    }
    else {
      (*tailp)->next = (CellStoreMaintenanceData *)arena.alloc(sizeof(CellStoreMaintenanceData));
      (*tailp)->next->cs = m_stores[i].cs.get();
      tailp = &(*tailp)->next;
    }
    m_stores[i].cs->get_index_memory_stats( &(*tailp)->index_stats );
    mdata->block_index_memory += ((*tailp)->index_stats).block_index_memory;
    mdata->bloom_filter_memory += ((*tailp)->index_stats).bloom_filter_memory;

    mdata->bloom_filter_accesses += m_stores[i].bloom_filter_accesses;
    mdata->bloom_filter_maybes += m_stores[i].bloom_filter_maybes;
    mdata->bloom_filter_fps += m_stores[i].bloom_filter_fps;
    if (!m_in_memory) {
      mdata->cell_count += m_stores[i].cell_count;
      mdata->key_bytes += m_stores[i].key_bytes;
      mdata->value_bytes += m_stores[i].value_bytes;
    }

    if (m_stores[i].shadow_cache) {
      (*tailp)->shadow_cache_size = m_stores[i].shadow_cache->memory_allocated();
      (*tailp)->shadow_cache_ecr  = m_stores[i].shadow_cache_ecr;
      (*tailp)->shadow_cache_hits = m_stores[i].shadow_cache_hits;
    }
    else {
      (*tailp)->shadow_cache_size = 0;
      (*tailp)->shadow_cache_ecr  = TIMESTAMP_MAX;
      (*tailp)->shadow_cache_hits = 0;
    }
    (*tailp)->maintenance_flags = 0;
    (*tailp)->next = 0;

    mdata->shadow_cache_memory += (*tailp)->shadow_cache_size;
  }
  mdata->file_count = m_stores.size();

  mdata->gc_needed = m_garbage_tracker.check_needed(mdata->deletes, mdata->mem_used, now);
  mdata->needs_merging = m_needs_merging;

  mdata->maintenance_flags = 0;

  return mdata;
}


void AccessGroup::add_cell_store(CellStorePtr &cellstore) {
  ScopedLock lock(m_mutex);

  m_disk_usage += cellstore->disk_usage();

  // Record the latest stored revision
  int64_t revision = boost::any_cast<int64_t>
    (cellstore->get_trailer()->get("revision"));
  if (revision > m_latest_stored_revision)
    m_latest_stored_revision = revision;

  if (m_in_memory) {
    HT_ASSERT(m_stores.empty());
    ScanContextPtr scan_context = new ScanContext(m_schema);
    CellListScannerPtr scanner = cellstore->create_scanner(scan_context);
    ByteString key, value;
    Key key_comps;
    m_cell_cache = new CellCache();
    while (scanner->get(key_comps, value)) {
      m_cell_cache->add(key_comps, value);
      scanner->forward();
    }
  }

  m_stores.push_back( cellstore );

  int64_t total_index_entries = 0;
  recompute_compression_ratio(&total_index_entries);

  m_garbage_tracker.accumulate_expirable( m_stores.back().expirable_data );

  m_file_tracker.add_live_noupdate(cellstore->get_filename(), total_index_entries);
}

void AccessGroup::compute_garbage_stats(uint64_t *input_bytesp, uint64_t *output_bytesp) {
  ScanContextPtr scan_context = new ScanContext(m_schema);
  MergeScannerPtr mscanner = new MergeScannerAccessGroup(scan_context);
  ByteString value;
  Key key;

  mscanner->add_scanner(m_immutable_cache->create_scanner(scan_context));

  if (!m_in_memory) {
    for (size_t i=0; i<m_stores.size(); i++) {
      HT_ASSERT(m_stores[i].cs);
      mscanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
    }
  }

  while (mscanner->get(key, value))
    mscanner->forward();

  mscanner->get_io_accounting_data(input_bytesp, output_bytesp);

}


void AccessGroup::run_compaction(int maintenance_flags) {
  ByteString bskey;
  ByteString value;
  Key key;
  CellListScannerPtr scanner;
  MergeScanner *mscanner = 0;
  CellStorePtr cellstore;
  CellCachePtr filtered_cache, shadow_cache;
  String metadata_key_str;
  bool abort_loop = true;
  bool minor = false;
  bool merging = false;
  bool major = false;
  bool gc = false;
  bool garbage_check_performed = false;
  size_t merge_offset=0, merge_length=0;
  String added_file;

  while (abort_loop) {
    ScopedLock lock(m_mutex);
    if (m_in_memory) {
      if (!m_immutable_cache || m_immutable_cache->empty())
        break;
      HT_INFOF("Starting InMemory Compaction of %s(%s)",
               m_range_name.c_str(), m_name.c_str());
    }
    else if (MaintenanceFlag::major_compaction(maintenance_flags) ||
             MaintenanceFlag::move_compaction(maintenance_flags)) {
      if ((!m_immutable_cache || m_immutable_cache->empty()) &&
          m_stores.size() <= (size_t)1 &&
          (!MaintenanceFlag::split(maintenance_flags) &&
           !MaintenanceFlag::move_compaction(maintenance_flags)))
        break;
      major = true;
      HT_INFOF("Starting Major Compaction of %s(%s)",
               m_range_name.c_str(), m_name.c_str());
    }
    else {
      if (MaintenanceFlag::merging_compaction(maintenance_flags)) {
        if (!find_merge_run(&merge_offset, &merge_length)) {
          m_needs_merging = false;
          break;
        }
        HT_INFOF("Starting Merging Compaction of %s(%s)",
                 m_range_name.c_str(), m_name.c_str());
        merging = true;
        merge_caches();
      }
      else {
        if (!MaintenanceFlag::gc_compaction(maintenance_flags) &&
            (!m_immutable_cache || m_immutable_cache->empty()))
          break;
        if (MaintenanceFlag::gc_compaction(maintenance_flags))
          gc = true;
        else
          minor = true;
        HT_INFOF("Starting %s Compaction of %s(%s)", (gc ? "GC" : "Minor"),
                 m_range_name.c_str(), m_name.c_str());
      }
    }
    abort_loop = false;
  }

  if (abort_loop) {
    ScopedLock lock(m_mutex);
    merge_caches();
    return;
  }

  try {

    String cs_file;

    int64_t max_num_entries = 0;

    {
      ScopedLock lock(m_mutex);
      ScanContextPtr scan_context = new ScanContext(m_schema);

      cs_file = format("%s/tables/%s/%s/%s/cs%d",
                       Global::toplevel_dir.c_str(),
                       m_identifier.id, m_name.c_str(),
                       m_range_dir.c_str(),
                       m_next_cs_id++);

      /**
       * Check for garbage and if threshold reached, change minor to major
       * compaction
       */
      if (minor) {
        if (m_immutable_cache) {
          if (m_garbage_tracker.check_needed( m_immutable_cache->memory_used() )) {
            uint64_t total_bytes, valid_bytes;
            compute_garbage_stats(&total_bytes, &valid_bytes);
            garbage_check_performed = true;
            m_garbage_tracker.set_garbage_stats(total_bytes, valid_bytes);
            if (m_garbage_tracker.need_collection()) {
              HT_INFOF("Switching from minor to major compaction to collect %.2f%% garbage",
                       ((double)(total_bytes-valid_bytes)/(double)total_bytes)*100.00);
              gc = true;
              minor = false;
            }
          }
        }
        else {
          merge_caches();
          return;
        }
      }

      cellstore = new CellStoreV5(Global::dfs.get(), m_schema.get());

      max_num_entries = m_immutable_cache ? m_immutable_cache->size() : 0;

      if (m_in_memory) {
        mscanner = new MergeScannerAccessGroup(scan_context);
        scanner = mscanner;
        mscanner->add_scanner(m_immutable_cache->create_scanner(scan_context));
        filtered_cache = new CellCache();
      }
      else if (merging) {
        mscanner = new MergeScannerAccessGroup(scan_context, true);
        scanner = mscanner;
        max_num_entries = 0;
        for (size_t i=merge_offset; i<merge_offset+merge_length; i++) {
          HT_ASSERT(m_stores[i].cs);
          mscanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
          int divisor = (boost::any_cast<uint32_t>(m_stores[i].cs->get_trailer()->get("flags")) & CellStoreTrailerV5::SPLIT) ? 2: 1;
          max_num_entries += (boost::any_cast<int64_t>
              (m_stores[i].cs->get_trailer()->get("total_entries")))/divisor;
        }
      }
      else if (major || gc) {
        mscanner = new MergeScannerAccessGroup(scan_context);
        scanner = mscanner;
        if (m_immutable_cache) {
          mscanner->add_scanner(m_immutable_cache->create_scanner(scan_context));
        }
        for (size_t i=0; i<m_stores.size(); i++) {
          HT_ASSERT(m_stores[i].cs);
          mscanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
          int divisor = (boost::any_cast<uint32_t>(m_stores[i].cs->get_trailer()->get("flags")) & CellStoreTrailerV5::SPLIT) ? 2: 1;
          max_num_entries += (boost::any_cast<int64_t>
              (m_stores[i].cs->get_trailer()->get("total_entries")))/divisor;
        }
      }
      else
        scanner = m_immutable_cache->create_scanner(scan_context);
    }

    cellstore->create(cs_file.c_str(), max_num_entries, m_cellstore_props, &m_identifier);

    while (scanner->get(key, value)) {
      cellstore->add(key, value);
      if (m_in_memory)
        filtered_cache->add(key, value);
      scanner->forward();
    }

    CellStoreTrailerV5 *trailer = dynamic_cast<CellStoreTrailerV5 *>(cellstore->get_trailer());

    if (major && mscanner)
      trailer->flags |= CellStoreTrailerV5::MAJOR_COMPACTION;

    if (maintenance_flags & MaintenanceFlag::SPLIT)
      trailer->flags |= CellStoreTrailerV5::SPLIT;

    cellstore->finalize(&m_identifier);

    /**
     * Install new CellCache and CellStore and update Live file tracker
     */
    std::vector<String> removed_files;
    int64_t total_index_entries = 0;
    {
      ScopedLock lock(m_mutex);

      if (merging) {
        std::vector<CellStoreInfo> new_stores;
        new_stores.reserve(m_stores.size() - (merge_length-1));
        for (size_t i=0; i<merge_offset; i++)
          new_stores.push_back(m_stores[i]);
        for (size_t i=merge_offset; i<merge_offset+merge_length; i++)
          removed_files.push_back(m_stores[i].cs->get_filename());
        new_stores.push_back(cellstore);
        added_file = cellstore->get_filename();
        for (size_t i=merge_offset+merge_length; i<m_stores.size(); i++)
          new_stores.push_back(m_stores[i]);
        m_stores.swap(new_stores);
      }
      else {

        /**
         * If major compaction was performed and we didn't do a garbage
         * check, then update the garbage tracker with statistics from
         * the MergeScanner.  Also clear the garbage tracker.
         */
        if ((major || gc) && mscanner) {
          if (!garbage_check_performed) {
            uint64_t input_bytes, output_bytes;
            mscanner->get_io_accounting_data(&input_bytes, &output_bytes);
            m_garbage_tracker.set_garbage_stats(input_bytes, output_bytes);
          }
          m_garbage_tracker.clear();
        }
        else if (m_in_memory)
          m_garbage_tracker.clear();

        m_latest_stored_revision = boost::any_cast<int64_t>
          (cellstore->get_trailer()->get("revision"));
        if (m_latest_stored_revision >= m_earliest_cached_revision) {
          HT_ERROR("Revision (clock) skew detected! May result in data loss.");
          HT_DEBUG_OUT << "Revision (clock) skew detected! May result in data"
              << " loss in AG for range " << m_range_name
              << " " << std::hex << this << HT_END;
        }
        if (m_in_memory) {
          m_immutable_cache = filtered_cache;
          merge_caches(false);
          for (size_t i=0; i<m_stores.size(); i++)
            removed_files.push_back(m_stores[i].cs->get_filename());
          m_stores.clear();
        }
        else {

          if (minor && Global::enable_shadow_cache &&
              !MaintenanceFlag::purge_shadow_cache(maintenance_flags))
            shadow_cache = m_immutable_cache;
          m_immutable_cache = 0;

          /** Drop the compacted CellStores from the stores vector **/
          if (major || gc) {
            for (size_t i=0; i<m_stores.size(); i++)
              removed_files.push_back(m_stores[i].cs->get_filename());
            m_stores.clear();
          }
        }

        /** Add the new cell store to the table vector, or delete it if
         * it contains no entries
         */
        if (cellstore->get_total_entries() > 0) {
          m_stores.push_back( CellStoreInfo(cellstore, shadow_cache, m_earliest_cached_revision_saved) );
          m_needs_merging = needs_merging();
          m_garbage_tracker.accumulate_expirable( m_stores.back().expirable_data );
          added_file = cellstore->get_filename();
        }
        else {
          String fname = cellstore->get_filename();
          cellstore = 0;
          try {
            Global::dfs->remove(fname);
          }
          catch (Hypertable::Exception &e) {
            HT_ERROR_OUT << "Problem removing '" << fname.c_str() << "' " \
                         << e << HT_END;
          }
        }
      }

      recompute_compression_ratio(&total_index_entries);
    }

    m_file_tracker.update_live(added_file, removed_files, m_next_cs_id, total_index_entries);
    m_file_tracker.update_files_column();

    if (merging)
      m_needs_merging = find_merge_run();
    else
      m_earliest_cached_revision_saved = TIMESTAMP_MAX;

    HT_INFOF("Finished Compaction of %s(%s) to %s", m_range_name.c_str(),
             m_name.c_str(), added_file.c_str());

  }
  catch (Exception &e) {
    HT_ERROR_OUT << m_range_name << "(" << m_name << ") " << e << HT_END;
    throw;
  }

}



/**
 *
 */
void AccessGroup::shrink(String &split_row, bool drop_high) {
  ScopedLock lock(m_mutex);
  CellCachePtr old_cell_cache = m_cell_cache;
  CellCachePtr new_cell_cache;
  ScanContextPtr scan_context = new ScanContext(m_schema);
  CellListScannerPtr cell_cache_scanner;
  ByteString key;
  ByteString value;
  Key key_comps;
  std::vector<CellStoreInfo> new_stores;
  CellStore *new_cell_store;
  uint64_t memory_added = 0;
  uint64_t items_added = 0;
  int cmp;

  m_recovering = true;

  m_earliest_cached_revision_saved = m_earliest_cached_revision;
  m_earliest_cached_revision = TIMESTAMP_MAX;

  try {

    if (drop_high) {
      m_end_row = split_row;
      range_dir_initialize();
    }
    else
      m_start_row = split_row;

    m_range_name = m_table_name + "[" + m_start_row + ".." + m_end_row + "]";
    m_full_name = m_range_name + "(" + m_name + ")";

    m_file_tracker.change_range(m_start_row, m_end_row);

    new_cell_cache = new CellCache();
    new_cell_cache->lock();
    m_cell_cache = new_cell_cache;

    if (old_cell_cache) {

      cell_cache_scanner = old_cell_cache->create_scanner(scan_context);

      /**
       * Shrink the CellCache
       */
      while (cell_cache_scanner->get(key_comps, value)) {

        cmp = strcmp(key_comps.row, split_row.c_str());

        if ((cmp > 0 && !drop_high) || (cmp <= 0 && drop_high)) {
          /*
           * For IN_MEMORY access groups, record earliest cached
           * revision that is > latest_stored.  For normal access groups,
           * record absolute earliest cached revision
           */
          if (m_in_memory) {
            if (key_comps.revision > m_latest_stored_revision &&
                key_comps.revision < m_earliest_cached_revision)
              m_earliest_cached_revision = key_comps.revision;
          }
          else if (key_comps.revision < m_earliest_cached_revision)
            m_earliest_cached_revision = key_comps.revision;
          add(key_comps, value);
          memory_added += key.length() + value.length();
          items_added++;
        }
        cell_cache_scanner->forward();
      }
    }

    new_cell_cache->unlock();

    /**
     * Shrink the CellStores
     */
    for (size_t i=0; i<m_stores.size(); i++) {
      String filename = m_stores[i].cs->get_filename();
      new_cell_store = CellStoreFactory::open(filename, m_start_row.c_str(), m_end_row.c_str());
      new_stores.push_back( new_cell_store );
    }

    m_stores = new_stores;

    m_needs_merging = find_merge_run();

    m_earliest_cached_revision_saved = TIMESTAMP_MAX;

    m_recovering = false;
  }
  catch (Exception &e) {
    m_recovering = false;
    m_cell_cache = old_cell_cache;
    m_earliest_cached_revision = m_earliest_cached_revision_saved;
    m_earliest_cached_revision_saved = TIMESTAMP_MAX;
    throw;
  }
}



/**
 */
void AccessGroup::release_files(const std::vector<String> &files) {
  {
    ScopedLock lock(m_outstanding_scanner_mutex);
    HT_ASSERT(m_outstanding_scanner_count > 0);
    m_outstanding_scanner_count--;
  }
  m_file_tracker.remove_references(files);
  m_file_tracker.update_files_column();
}


void AccessGroup::stage_compaction() {
  ScopedLock lock(m_mutex);
  if (!m_cell_cache)
    return;
  HT_ASSERT(!m_immutable_cache);
  m_immutable_cache = m_cell_cache;
  m_immutable_cache->freeze();
  m_garbage_tracker.add_delete_count( m_immutable_cache->get_delete_count() );
  m_garbage_tracker.accumulate_data( m_immutable_cache->memory_used() );

  m_cell_cache = 0;
  m_earliest_cached_revision_saved = m_earliest_cached_revision;
  m_earliest_cached_revision = TIMESTAMP_MAX;
}


void AccessGroup::unstage_compaction() {
  ScopedLock lock(m_mutex);
  m_garbage_tracker.add_delete_count( -m_immutable_cache->get_delete_count() );
  m_garbage_tracker.accumulate_data( -m_immutable_cache->memory_used() );
  merge_caches();
}


/**
 * Assumes mutex is locked
 */
void AccessGroup::merge_caches(bool reset_earliest_cached_revision) {

  if (reset_earliest_cached_revision &&
      m_earliest_cached_revision_saved != TIMESTAMP_MAX) {
    m_earliest_cached_revision = m_earliest_cached_revision_saved;
    m_earliest_cached_revision_saved = TIMESTAMP_MAX;
  }

  if (!m_immutable_cache)
    return;

  if (m_immutable_cache->size() == 0) {
    m_immutable_cache = 0;
    return;
  }
  else if (!m_cell_cache || m_cell_cache->size() == 0) {
    m_cell_cache = m_immutable_cache;
    m_cell_cache->unfreeze();
    m_immutable_cache = 0;
    return;
  }

  Key key;
  ByteString value;
  CellCachePtr merged_cache = new CellCache();
  ScanContextPtr scan_context = new ScanContext(m_schema);
  CellListScannerPtr scanner = m_immutable_cache->create_scanner(scan_context);
  while (scanner->get(key, value)) {
    merged_cache->add(key, value);
    scanner->forward();
  }

  // Add cell cache
  scanner = m_cell_cache->create_scanner(scan_context);
  while (scanner->get(key, value)) {
    merged_cache->add(key, value);
    scanner->forward();
  }
  m_immutable_cache = 0;
  m_cell_cache = merged_cache;
}

extern "C" {
#include <unistd.h>
}

void AccessGroup::range_dir_initialize() {
  char hash_str[33];
  if (m_end_row == "")
    memset(hash_str, '0', 16);
  else
    md5_trunc_modified_base64(m_end_row.c_str(), hash_str);
  hash_str[16] = 0;
  m_range_dir = hash_str;

  String abs_range_dir = format("%s/tables/%s/%s/%s",
                                Global::toplevel_dir.c_str(),
                                m_identifier.id, m_name.c_str(),
                                m_range_dir.c_str());

  m_next_cs_id = 0;

  try {
    if (!Global::dfs->exists(abs_range_dir))
      Global::dfs->mkdirs(abs_range_dir);
    else {
      uint32_t id;
      vector<String> listing;
      Global::dfs->readdir(abs_range_dir, listing);
      for (size_t i=0; i<listing.size(); i++) {
        const char *fname = listing[i].c_str();
        if (!strncmp(fname, "cs", 2)) {
          id = atoi(&fname[2]);
          if (id >= m_next_cs_id)
            m_next_cs_id = id+1;
        }
      }
    }
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }

  /**
  HT_INFOF("Just initialized %s[%s..%s](%s) (pid=%d) to ID %u",
           m_table_name.c_str(), m_start_row.c_str(),
           m_end_row.c_str(), m_name.c_str(), (int)getpid(), (unsigned)m_next_cs_id);
  */

}

void AccessGroup::recompute_compression_ratio(int64_t *total_index_entriesp) {
  m_disk_usage = 0;
  m_compression_ratio = 0.0;
  if (total_index_entriesp)
    *total_index_entriesp = 0;
  for (size_t i=0; i<m_stores.size(); i++) {
    HT_ASSERT(m_stores[i].cs);
    if (total_index_entriesp)
      *total_index_entriesp += (int64_t)m_stores[i].index_entries;
    double disk_usage = m_stores[i].cs->disk_usage();
    m_disk_usage += (uint64_t)disk_usage;
    m_compression_ratio += disk_usage / m_stores[i].cs->compression_ratio();
  }
  if (m_disk_usage != 0)
    m_compression_ratio = (double)m_disk_usage / m_compression_ratio;
  else
    m_compression_ratio = 1.0;
}


bool AccessGroup::find_merge_run(size_t *indexp, size_t *lenp) {
  size_t index = 0;
  size_t count = 0;
  size_t i = 0;
  int64_t running_total = 0;

  if (m_in_memory || m_stores.size() == 0)
    return false;

  do {
    count++;
    running_total += m_stores[i].cs->disk_usage();

    if (running_total >= Global::cellstore_target_size_max) {
      if (count > (size_t)Global::merge_cellstore_run_length_threshold) {
        if (indexp)
          *indexp = index;
        if (lenp)
           *lenp = count-1;
        return true;
      }
      index = i+1;
      count = 0;
      running_total = 0;
    }
    else if (running_total >= Global::cellstore_target_size_min &&
             count > 1) {
      if (indexp)
        *indexp = index;
      if (lenp)
        *lenp = count;
      return true;
    }
    i++;
  } while (i < m_stores.size());

  if (count > (size_t)Global::merge_cellstore_run_length_threshold) {
    if (indexp)
      *indexp = index;
    if (lenp)
      *lenp = count;
    return true;
  }

  return false;
}


bool AccessGroup::needs_merging() {
  size_t count = 0;
  int i = 0;
  int64_t running_total = 0;

  if (m_in_memory || m_stores.size() == 0)
    return false;

  for (i = m_stores.size()-1; i>=0; i--) {
    count++;
    running_total += m_stores[i].cs->disk_usage();
    if (running_total >= Global::cellstore_target_size_max)
      break;
    else if (running_total >= Global::cellstore_target_size_min &&
             (m_stores.size() - i) > 1)
      return true;
  }

  if (i < 0 && count > (size_t)Global::merge_cellstore_run_length_threshold)
    return true;

  /** Search from the beginning **/

  i = 0;
  count = 0;
  running_total = 0;
  do {
    count++;
    running_total += m_stores[i].cs->disk_usage();

    if (running_total >= Global::cellstore_target_size_max) {
      if (count > (size_t)Global::merge_cellstore_run_length_threshold)
        return true;
      count = 0;
      running_total = 0;
    }
    else if (running_total >= Global::cellstore_target_size_min &&
             count > 1) {
      return true;
    }
    i++;
  } while (i < (int)m_stores.size());

  if (count > (size_t)Global::merge_cellstore_run_length_threshold)
    return true;

  return false;
}

namespace {
  struct LtCellStoreInfoTimestamp {
    bool operator()(const CellStoreInfo &x, const CellStoreInfo &y) const {
      return x.timestamp_min < y.timestamp_min;
    }
  };
}


void AccessGroup::sort_cellstores_by_timestamp() {
  LtCellStoreInfoTimestamp order;
  sort(m_stores.begin(), m_stores.end(), order);
}


void AccessGroup::dump_keys(std::ofstream &out) {
  ScopedLock lock(m_mutex);
  Schema::ColumnFamily *cf;
  const char *family;
  KeySet keys;

  // write header line
  out << "\n" << m_full_name << " Keys:\n";

  if (m_immutable_cache)
    m_immutable_cache->populate_key_set(keys);

  if (m_cell_cache)
    m_cell_cache->populate_key_set(keys);

  for (KeySet::iterator iter = keys.begin();
       iter != keys.end(); ++iter) {
    if ((cf = m_schema->get_column_family((*iter).column_family_code, true)))
      family = cf->name.c_str();
    else
      family = "UNKNOWN";
    out << (*iter).row << " " << family;
    if (*(*iter).column_qualifier)
      out << ":" << (*iter).column_qualifier;
    out << " 0x" << std::hex << (int)(*iter).flag << std::dec
	<< " ts=" << (*iter).timestamp
	<< " rev=" << (*iter).revision << "\n";
  }
}


std::ostream &Hypertable::operator<<(std::ostream &os, const AccessGroup::MaintenanceData &mdata) {
  os << "ACCESS GROUP " << mdata.ag->get_full_name() << "\n";
  os << "earliest_cached_revision=" << mdata.earliest_cached_revision << "\n";
  os << "latest_stored_revision=" << mdata.latest_stored_revision << "\n";
  os << "mem_used=" << mdata.mem_used << "\n";
  os << "mem_allocated=" << mdata.mem_allocated << "\n";
  os << "cached_items=" << mdata.cached_items << "\n";
  os << "cell_count=" << mdata.cell_count << "\n";
  os << "immutable_items=" << mdata.immutable_items << "\n";
  os << "disk_used=" << mdata.disk_used << "\n";
  os << "disk_estimate=" << mdata.disk_estimate << "\n";
  os << "log_space_pinned=" << mdata.log_space_pinned << "\n";
  os << "key_bytes=" << mdata.key_bytes << "\n";
  os << "value_bytes=" << mdata.value_bytes << "\n";
  os << "file_count=" << mdata.file_count << "\n";
  os << "deletes=" << mdata.deletes << "\n";
  os << "outstanding_scanners=" << mdata.outstanding_scanners << "\n";
  os << "compression_ratio=" << mdata.compression_ratio << "\n";
  os << "maintenance_flags=" << mdata.maintenance_flags << "\n";
  os << "block_index_memory=" << mdata.block_index_memory << "\n";
  os << "bloom_filter_memory=" << mdata.bloom_filter_memory << "\n";
  os << "bloom_filter_accesses=" << mdata.bloom_filter_accesses << "\n";
  os << "bloom_filter_maybes=" << mdata.bloom_filter_maybes << "\n";
  os << "bloom_filter_fps=" << mdata.bloom_filter_fps << "\n";
  os << "shadow_cache_memory=" << mdata.shadow_cache_memory << "\n";
  os << "in_memory=" << (mdata.in_memory ? "true" : "false") << "\n";
  os << "gc_needed=" << (mdata.gc_needed ? "true" : "false") << "\n";
  os << "needs_merging=" << (mdata.needs_merging ? "true" : "false") << "\n";
  return os;
}
