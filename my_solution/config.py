# config.py

# Data paths
DATA_ARCH_GLOB      = "data_arch/*.csv"
PARTITIONED_DIR     = "partitioned"
ANALYSIS_DIR        = "analysis"
LOITERING_DIR       = "loitering"

CHUNK_SIZE          = 50000      
NUM_WORKERS         = 11         

DIRTY_MMSI          = {"000000000", "111111111", "123456789"}
DIRTY_PREFIXES      = ("111",)
DOWNSAMPLE_MINUTES  = 2    
EXCLUDED_SHIP_TYPES = {"tug", "towing", "sar"}      

# Baltic & North Sea Bounding Box
LAT_MIN = 51.0
LAT_MAX = 66.0
LON_MIN = -5.0
LON_MAX = 30.0

# Anomaly A - minimum AIS gap in hours to flag:
GOING_DARK_HOURS    = 4.0        

# Anomaly B: 
LOITER_SPEED_KNOTS  = 1.0        
LOITER_GRID_DEG     = 0.02     
LOITER_PROX_M       = 500        
LOITER_MIN_HOURS    = 2.0        

# Anomaly C:
DRAFT_MIN_HOURS     = 2.0        
DRAFT_CHANGE_PCT    = 0.05       # Min fractional change in draught to flag

# Anomaly D:
TELEPORT_KNOTS      = 60.0      

# --- DFSI Weights ---
DFSI_WEIGHTS        = {"A": 3, "B": 4, "C": 2, "D": 5}

# --- Output ---
TOP_N_VESSELS       = 10
