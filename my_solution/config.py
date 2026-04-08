# config.py
# Central configuration for all thresholds and paths

# --- Data Paths ---
DATA_ARCH_GLOB      = "data_arch/*.csv"
PARTITIONED_DIR     = "partitioned"
ANALYSIS_DIR        = "analysis"
LOITERING_DIR       = "loitering"

# --- Partition Settings ---
CHUNK_SIZE          = 50000      # Rows per shard

# --- Worker Settings ---
NUM_WORKERS         = 11          # Set to 1 to measure baseline, increase for speedup

# --- Parsing / Validation ---
DIRTY_MMSI          = {"000000000", "111111111", "123456789"}
DIRTY_PREFIXES      = ("111",)
DOWNSAMPLE_MINUTES  = 2    
EXCLUDED_SHIP_TYPES = {"tug", "towing", "sar"}      # Keep one point per N minutes per vessel

# Baltic & North Sea Bounding Box
LAT_MIN = 51.0
LAT_MAX = 66.0
LON_MIN = -5.0
LON_MAX = 30.0

# --- Anomaly A: Going Dark ---
GOING_DARK_HOURS    = 4.0        # Minimum AIS gap in hours to flag
# Any movement during the gap is suspicious — no distance threshold needed
# We flag if the vessel reappears at ANY different location after the gap

# --- Anomaly B: Loitering / Rendezvous ---
LOITER_SPEED_KNOTS  = 1.0        # Max SOG to be considered loitering
LOITER_GRID_DEG     = 0.02       # Grid cell size in degrees (~2km)
LOITER_PROX_M       = 500        # Max distance between two vessels to be a pair
LOITER_MIN_HOURS    = 2.0        # Min duration of encounter to flag

# --- Anomaly C: Draft Change at Sea ---
DRAFT_MIN_HOURS     = 2.0        # Min hours between two points to check draft
DRAFT_CHANGE_PCT    = 0.05       # Min fractional change in draught to flag (5%)

# --- Anomaly D: Teleportation ---
TELEPORT_KNOTS      = 60.0       # Implied speed above this is physically impossible

# --- DFSI Weights ---
DFSI_WEIGHTS        = {"A": 3, "B": 4, "C": 2, "D": 5}

# --- Output ---
TOP_N_VESSELS       = 10
