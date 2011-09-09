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
#include "Common/Serialization.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"
extern "C" {
#include "Common/md5.h"
}
#include "Types.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

const char *TableIdentifier::METADATA_ID = "0/0";
const char *TableIdentifier::METADATA_NAME= "sys/METADATA";
const int TableIdentifier::METADATA_ID_LENGTH = 3;

bool TableIdentifier::operator==(const TableIdentifier &other) const {
  if (id == 0 || other.id == 0) {
    if (id != other.id)
      return false;
  }
  if (strcmp(id, other.id) ||
      generation != other.generation)
    return false;
  return true;
}

bool TableIdentifier::operator!=(const TableIdentifier &other) const {
  return !(*this == other);
}

bool TableIdentifier::operator<(const TableIdentifier &other) const {
  if (id == 0 || other.id == 0) {
    if (other.id != 0)
      return true;
    else if (id != 0)
      return false;
  }
  int cmpval = strcmp(id, other.id);

  if (cmpval < 0 ||
      (cmpval == 0 && generation < other.generation))
    return true;
  return false;
}


size_t TableIdentifier::encoded_length() const {
  return 4 + encoded_length_vstr(id);
}

void TableIdentifier::encode(uint8_t **bufp) const {
  encode_vstr(bufp, id);
  encode_i32(bufp, generation);
}

void TableIdentifier::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding table identitier",
    id = decode_vstr(bufp, remainp);
    generation = decode_i32(bufp, remainp));
}

void TableIdentifierManaged::decode(const uint8_t **bufp, size_t *remainp) {
  TableIdentifier::decode(bufp, remainp);
  *this = *this;
}

String RangeSpec::type_str(int type) {
  switch(type) {
  case (ROOT):
    return (String) "root";
  case (METADATA):
    return (String) "metadata";
  case (SYSTEM):
    return (String) "system";
  case (USER):
    return "user";
  case (UNKNOWN):
    return "unknown";
  }
  return "unknown";
}

bool RangeSpec::operator==(const RangeSpec &other) const {

  bool start_null = (start_row == 0) || (strlen(start_row)==0);
  bool other_start_null = (other.start_row == 0) || (strlen(other.start_row)==0);


  if (start_null || other_start_null) {
    if (start_null != other_start_null)
      return false;
  }
  else {
    if (strcmp(start_row, other.start_row))
      return false;
  }
  if (end_row == 0 || other.end_row == 0) {
    if (end_row != other.end_row)
      return false;
  }
  else {
    if (strcmp(end_row, other.end_row))
      return false;
  }
  return true;
}

bool RangeSpec::operator!=(const RangeSpec &other) const {
  return !(*this == other);
}

bool RangeSpec::operator<(const RangeSpec &other) const {
  bool end_null = (end_row==0) || (strlen(end_row)==0);
  bool other_end_null = (other.end_row == 0) || (strlen(other.end_row)==0);

  if (end_null || other_end_null) {
    if (!other_end_null)
      return true;
    else if (!end_null)
      return false;
  }
  else {
    int cmpval = strcmp(end_row, other.end_row);
    if (cmpval < 0)
      return true;
    else if (cmpval > 0)
      return false;
  }

  bool start_null = (start_row == 0) || (strlen(start_row)==0);
  bool other_start_null = (other.start_row == 0) || (strlen(other.start_row)==0);

  if (start_null || other_start_null) {
    if (!other_start_null)
      return true;
    else if (!start_null)
      return false;
  }
  else {
    int cmpval = strcmp(start_row, other.start_row);
    if (cmpval < 0)
      return true;
    else if (cmpval > 0)
      return false;
  }
  return false;
}

size_t RangeSpec::encoded_length() const {
  return encoded_length_vstr(start_row) + encoded_length_vstr(end_row);
}

void RangeSpec::encode(uint8_t **bufp) const {
  encode_vstr(bufp, start_row);
  encode_vstr(bufp, end_row);
}

void RangeSpec::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding range spec",
    start_row = decode_vstr(bufp, remainp);
    end_row = decode_vstr(bufp, remainp));
}

void RangeSpecManaged::decode(const uint8_t **bufp, size_t *remainp) {
  RangeSpec::decode(bufp, remainp);
  *this = *this;
}

bool QualifiedRangeSpec::is_root() const {
  bool start_null = (range.start_row == 0) || (strlen(range.start_row)==0);
  return (table.is_metadata() && start_null);
}

bool QualifiedRangeSpec::operator<(const QualifiedRangeSpec &other) const {
  if (table == other.table)
    return (range < other.range);
  else
    return (table < other.table);
}

bool QualifiedRangeSpec::operator==(const QualifiedRangeSpec &other) const {
  return (table == other.table && range == other.range);
}

size_t QualifiedRangeSpec::encoded_length() const {
  return (table.encoded_length() + range.encoded_length());
}

void QualifiedRangeSpec::encode(uint8_t **bufp) const {
  table.encode(bufp);
  range.encode(bufp);
}

void QualifiedRangeSpec::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding qualified range spec managed ",
    table.decode(bufp, remainp);
    range.decode(bufp, remainp));
}

bool QualifiedRangeStateSpec::is_root() const {
  return (qualified_range.is_root());
}

bool QualifiedRangeStateSpec::operator<(const QualifiedRangeStateSpec &other) const {
  return (qualified_range < other.qualified_range);
}

bool QualifiedRangeStateSpec::operator==(const QualifiedRangeStateSpec &other) const {
  return (qualified_range == other.qualified_range);
}

size_t QualifiedRangeStateSpec::encoded_length() const {
  return (qualified_range.encoded_length() + state.encoded_length());
}

void QualifiedRangeStateSpec::encode(uint8_t **bufp) const {
  qualified_range.encode(bufp);
  state.encode(bufp);
}

void QualifiedRangeStateSpec::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding qualified range spec managed ",
    qualified_range.decode(bufp, remainp);
    state.decode(bufp, remainp));
}

bool QualifiedRangeSpecManaged::operator<(const QualifiedRangeSpecManaged &other) const {
  if (table == other.table)
    return (range < other.range);
  else
    return (table < other.table);
}

size_t QualifiedRangeSpecManaged::encoded_length() const {
  return (m_table.encoded_length() + m_range.encoded_length());
}

void QualifiedRangeSpecManaged::encode(uint8_t **bufp) const {
  m_table.encode(bufp);
  m_range.encode(bufp);
}

void QualifiedRangeSpecManaged::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding qualified range spec managed ",
    m_table.decode(bufp, remainp);
    m_range.decode(bufp, remainp));
  table = m_table;
  range = m_range;
}

bool QualifiedRangeStateSpecManaged::operator<(const QualifiedRangeStateSpecManaged &other) const {
  return (qualified_range < other.qualified_range);
}

size_t QualifiedRangeStateSpecManaged::encoded_length() const {
  return (m_qualified_range.encoded_length() + m_state.encoded_length());
}

void QualifiedRangeStateSpecManaged::encode(uint8_t **bufp) const {
  m_qualified_range.encode(bufp);
  m_state.encode(bufp);
}

void QualifiedRangeStateSpecManaged::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding qualified range spec managed ",
    m_qualified_range.decode(bufp, remainp);
    m_state.decode(bufp, remainp));
  qualified_range = m_qualified_range;
  state = m_state;
}

ostream &Hypertable::operator<<(ostream &os, const TableIdentifier &tid) {
  os <<"{TableIdentifier: id='"<< tid.id
     <<"' generation="<< tid.generation <<"}";
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const RangeSpec &range) {
  os <<"{RangeSpec:";

  HT_DUMP_CSTR(os, start, range.start_row);
  HT_DUMP_CSTR(os, end, range.end_row);

  os <<'}';
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const QualifiedRangeSpec &qualified_range) {
  os << qualified_range.table << qualified_range.range;
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const QualifiedRangeSpecManaged &qualified_range) {
  os << qualified_range.m_table << qualified_range.m_range;
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const QualifiedRangeStateSpec &qrss) {
  os << "{QualifiedRangeStateSpec: " << qrss.qualified_range << qrss.state << "}";
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const QualifiedRangeStateSpecManaged &qrss) {
  os << "{QualifiedRangeStateSpecManaged: " << qrss.qualified_range << qrss.state << "}";
  return os;
}
