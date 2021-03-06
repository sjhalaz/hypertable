/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

#include "TableCallback.h"
#include "TableScanner.h"
#include "TableMutator.h"

namespace Hypertable {

void TableCallback::scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells) {
  m_scanner->scan_ok(cells);
}

void TableCallback::scan_error(TableScannerAsync *scanner, int error,
                                          const String &error_msg, bool eos) {
  m_scanner->scan_error(error, error_msg);
}

void TableCallback::update_ok(TableMutatorAsync *mutator) {
  m_mutator->update_ok();
}

void TableCallback::update_error(TableMutatorAsync *mutator, int error,
    FailedMutations &failures) {
  m_mutator->update_error(error, failures);
}

} // namespace Hypertable

