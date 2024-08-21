#pragma once

#include "sqlite3.h"

struct HitterData;
struct HitterStats;
struct HitterValue;

class Database
{
public:
	Database(const char* filename);
	void GetData(HitterData** hd, int* lenData, HitterStats** hs, int* lenStats, HitterValue** hv, int* lenValue);
private:
	// SQLite required variables
	sqlite3* db = nullptr;
	int rc = 0;
	char* zErrMsg = nullptr;

	// Variables used for callback function
	static int CallbackInt(void* param, int argc, char** argv, char** azColName);
	static int intResult;
	static int currentRow;
	static int CallbackHitterData(void* param, int argc, char** argv, char** azColName);
	static int CallbackHitterStats(void* param, int argc, char** argv, char** azColName);
	static int CallbackHitterValue(void* param, int argc, char** argv, char** azColName);
	static HitterData* data;
	static HitterStats* stats;
	static HitterValue* value;
};