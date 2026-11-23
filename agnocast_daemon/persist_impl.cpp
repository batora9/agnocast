// SPDX-License-Identifier: Apache-2.0
// No persistence (C): a restarted daemon rebuilds its metadata from the
// clients' reports alone.
#include "persist.hpp"

namespace persist
{

std::unique_ptr<Sink> make_sink()
{
  return nullptr;
}

}  // namespace persist
