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

#ifndef HYPERTABLE_DISPATCHHANDLERTIMEDUNBLOCK_H
#define HYPERTABLE_DISPATCHHANDLERTIMEDUNBLOCK_H

#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"

#include "Common/StringExt.h"

#include "Context.h"
#include "OperationProcessor.h"

namespace Hypertable {

  /**
   */
  class DispatchHandlerTimedUnblock: public DispatchHandler {

  public:

    DispatchHandlerTimedUnblock(ContextPtr &context, String &label) :
        m_context(context), m_label(label) { }

    virtual void handle(EventPtr &event) {
      HT_ASSERT(event->type == Event::TIMER);
      m_context->op->unblock(m_label);
    }

  private:
    ContextPtr m_context;
    String m_label;
  };

  typedef intrusive_ptr<DispatchHandlerTimedUnblock> DispatchHandlerTimedUnblockPtr;

}


#endif // HYPERTABLE_DISPATCHHANDLERTIMEDUNBLOCK_H
