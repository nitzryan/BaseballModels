#include "Database.h"
#include <stdio.h>
#include <stdlib.h>
#include "../Types.h"

int Database::intResult = 0;
int Database::currentRow = 0;
HitterData* Database::data = NULL;
HitterStats* Database::stats = NULL;

Database::Database(const char* filename)
{
	rc = sqlite3_open(filename, &db);
	if (rc != 0) {
		printf("Can't open database: %s\n", sqlite3_errmsg(db));
	}
}

void Database::GetData(HitterData** hd, int* lenData, HitterStats** hs, int* lenStats, HitterValue** hv, int* lenValue)
{
	rc = sqlite3_exec(db, "SELECT COUNT(*) FROM Model_Players WHERE isHitter='1'", Database::CallbackInt, nullptr, &zErrMsg);
	Database::data = new HitterData[Database::intResult]; //(HitterData*)malloc(Database::intResult * sizeof(HitterData));
	for (int i = 0; i < Database::intResult; i++) {
		data[i].lenStats = 0;
		data[i].lenValue = 0;
	}
	*lenData = Database::intResult;
	Database::currentRow = 0;
	const char* sqlString = "SELECT m.mlbId, m.ageAtSigningYear, p.draftPick "
		"FROM Model_Players AS m "
		"INNER JOIN Player AS p ON m.mlbId = p.mlbId " 
		"WHERE m.isHitter='1' "
		"AND p.position='hitting' "
		"ORDER BY m.mlbID ASC ";
	rc = sqlite3_exec(db, sqlString, Database::CallbackHitterData, nullptr, &zErrMsg);
	*hd = Database::data;

	rc = sqlite3_exec(db, "SELECT COUNT(*) FROM Model_HitterStats", Database::CallbackInt, nullptr, &zErrMsg);
	Database::stats = (HitterStats*)malloc(Database::intResult * sizeof(HitterStats));
	*lenStats = Database::intResult;
	Database::currentRow = 0;
	rc = sqlite3_exec(db, "SELECT * FROM Model_HitterStats ORDER BY mlbId ASC, Year ASC, Month ASC", Database::CallbackHitterStats, nullptr, &zErrMsg);
	*hs = Database::stats;

	/*rc = sqlite3_exec(db, "SELECT COUNT(*) FROM Model_PlayerWar WHERE isHitter='1'", Database::CallbackInt, nullptr, &zErrMsg);
	Database::value = (HitterValue*)malloc(Database::intResult * sizeof(HitterValue));
	*lenValue = Database::intResult;
	Database::currentRow = 0;
	rc = sqlite3_exec(db, "SELECT * FROM Model_PlayerWar WHERE isHitter='1' ORDER BY mlbID ASC, Year ASC", Database::CallbackHitterValue, nullptr, &zErrMsg);
	*hv = Database::value;*/
}

int Database::CallbackInt(void* param, int argc, char** argv, char** azColName)
{
	if (argc != 1) {
		printf("Too Many arguments for CallbackInt: %d given, 1 expected\n", argc);
		exit(1);
	}

	Database::intResult = atoi(argv[0]);

	return 0;
}

int Database::CallbackHitterData(void* param, int argc, char** argv, char** azColName)
{
	int mlbId = atoi(argv[0]);
	float ageAtSigning = atof(argv[1]);
	float wasDrafted = (argv[2] == NULL) ? 0 : 1;
	float draftPick;
	if (wasDrafted == 1) {
		draftPick = atof(argv[2]);
	}
	else {
		draftPick = 0;
	}

	HitterData* d = &Database::data[Database::currentRow];
	d->mlbId = mlbId;
	d->ageAtSigning = ageAtSigning;
	d->draftPick = draftPick;
	d->wasDrafted = wasDrafted;
	d->lenStats = 0;
	d->lenValue = 0;
	d->statsIdx = 0;
	d->valueIdx = 0;

	Database::currentRow++;
	return 0;
}

int Database::CallbackHitterStats(void* param, int argc, char** argv, char** azColName)
{
	if (Database::currentRow % 10000 == 0) {
		printf("Row %d\n", Database::currentRow);
	}
	
	int mlbId = atoi(argv[0]);
	// Look to see if this result exists in HitterData
	int idx = 0;
	while (true) {
		int hitterIdx = Database::data[idx].mlbId;
		if (hitterIdx == mlbId) { // Not the first entry for this player
			if (Database::data[idx].lenStats == 0) {
				Database::data[idx].statsIdx = Database::currentRow;
			}
			Database::data[idx].lenStats += 1;
			break;
		}
		idx++;
	}

	HitterStats& hs = stats[Database::currentRow];
	hs.month = atof(argv[2]);
	hs.pa = atof(argv[3]);
	hs.age = atof(argv[4]);
	hs.level = atof(argv[5]);
	hs.parkRunFactor = atof(argv[6]);
	hs.parkHrFactor = atof(argv[7]);
	hs.avgRatio = atof(argv[8]);
	hs.obpRatio = atof(argv[9]);
	hs.isoRatio = atof(argv[10]);
	hs.wobaRatio = atof(argv[11]);
	hs.sbRateRatio = atof(argv[12]);
	hs.sbPercRatio = atof(argv[13]);
	hs.hrPercRatio = atof(argv[14]);
	hs.bbPercRatio = atof(argv[15]);
	hs.kPercRatio = atof(argv[16]);
	hs.percC = atof(argv[17]);
	hs.perc1B = atof(argv[18]);
	hs.perc2B = atof(argv[19]);
	hs.perc3B = atof(argv[20]);
	hs.percSS = atof(argv[21]);
	hs.percLF = atof(argv[22]);
	hs.percCF = atof(argv[23]);
	hs.percRF = atof(argv[24]);
	hs.percDH = atof(argv[25]);

	Database::currentRow++;
	return 0;
}

int Database::CallbackHitterValue(void* param, int argc, char** argv, char** azColName)
{
	Database::currentRow++;
	return 0;
}
