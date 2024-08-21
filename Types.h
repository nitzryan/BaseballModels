#pragma once

struct HitterData {
    int mlbId;
    float ageAtSigning;
    float draftPick;
    float wasDrafted;
    int lenStats;
    int lenValue;
    int statsIdx;
    int valueIdx;
};

struct HitterStats {
    float month;
    float age;
    float pa;
    float level;
    float parkRunFactor;
    float parkHrFactor;
    float avgRatio;
    float obpRatio;
    float isoRatio;
    float wobaRatio;
    float sbRateRatio;
    float sbPercRatio;
    float hrPercRatio;
    float bbPercRatio;
    float kPercRatio;
    float percC;
    float perc1B;
    float perc2B;
    float perc3B;
    float percSS;
    float percLF;
    float percCF;
    float percRF;
    float percDH;
};

struct HitterValue {
    int pa;
    float war;
    float off;
    float def;
    float bsr;
};