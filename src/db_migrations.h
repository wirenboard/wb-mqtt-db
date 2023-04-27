#pragma once

#include "SQLiteCpp/SQLiteCpp.h"
#include <vector>

typedef void (*ConvertDbFnType)(SQLite::Database& db);

std::vector<ConvertDbFnType> GetMigrations();
