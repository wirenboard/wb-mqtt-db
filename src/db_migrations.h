#pragma once

#include <vector>
#include "SQLiteCpp/SQLiteCpp.h"

typedef void(*ConvertDbFnType)(SQLite::Database& db);

std::vector<ConvertDbFnType> GetMigrations();
