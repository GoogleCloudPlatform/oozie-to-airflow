source=$1
target=$2

sourceSnapshot="$source-SnapShot-"$(date '+%Y-%m-%d_%H-%M-%S')
targetSnapshot="$target-SnapShot-"$(date '+%Y-%m-%d_%H-%M-%S')

############################################################
##  -- START -- Some functions to execute hbase commands  ##
############################################################

disable_table () {
echo "[INFO] disable Table $1"
echo "disable '$1'" | hbase shell -n > /dev/null 2>&1
status=$?
return $status
}

enable_table () {
echo "[INFO] enable Table $1"
echo "enable '$1'" | hbase shell -n > /dev/null 2>&1
status=$?
return $status
}

delete_table () {
echo "[INFO] drop '$1'"
echo "drop '$1'" | hbase shell -n > /dev/null 2>&1
status=$?
return $status
}

take_snap () {
echo "[INFO] snapshot '$1', '$2'"
echo "snapshot '$1', '$2'" | hbase shell -n > /dev/null 2>&1
status=$?
return $status
}

clone_snap () {
echo "[INFO] clone_snapshot '$1', '$2'"
echo "clone_snapshot '$1', '$2'" | hbase shell -n > /dev/null 2>&1
status=$?
    return $status
}

delete_snap () {
echo "[INFO] delete_snapshot '$1'"
echo "delete_snapshot '$1'" | hbase shell -n > /dev/null 2>&1
status=$?
    return $status
}

restore_snap () {
echo "[INFO] restore_snap '$1'"
echo "restore_snapshot '$1'" | hbase shell -n > /dev/null 2>&1
status=$?
    return $status
}

############################################################
##    -- END -- Some functions to execute hbase commands  ##
############################################################

############################################################
##    8 steps to perform prod/recette snap from temp  ##
############################################################


# snap temporary table into prod/recette table
echo "[INFO] Start Snapshot"
# Step1: disable_table $target
echo "******************** Step1 ----- > disable_table $target"
if disable_table $target; then 
   echo "disable_table $target succeeded."
else
   echo "disable_table $target may have failed"
   exit
fi

# Step2: take_snap $target $targetSnapshot
echo "******************** Step2 ----- > take_snap $target $targetSnapshot"
if take_snap $target $targetSnapshot; then 
   echo "take_snap $target $targetSnapshot succeeded."
else
   echo "take_snap $target $targetSnapshot may have failed"
   enable_table $target
   exit
fi

# Step3: delete_table $target
echo "******************** Step3 ----- > delete_table $target"
if delete_table $target; then
   echo "delete_table $target succeeded."
else
   echo "delete_table $target may have failed"
   delete_snap $targetSnapshot
   enable_table $target
   exit
fi

# Step4: disable_table $source
echo "******************** Step4 ----- > disable_table $source"
if disable_table $source; then
   echo "disable_table $source succeeded."
else
   echo "disable_table $source may have failed"
   restore_snap $targetSnapshot
   delete_snap $targetSnapshot
   exit
fi

# Step5: take_snap $source $sourceSnapshot
echo "******************** Step5 ----- > take_snap $source $sourceSnapshot"
if take_snap $source $sourceSnapshot; then
   echo "take_snap $source $sourceSnapshot succeeded."
else
   echo "take_snap $source $sourceSnapshot may have failed"
   restore_snap $targetSnapshot
   delete_snap $targetSnapshot
   enable_table $source 
   exit
fi

# Step6: clone_snap $sourceSnapshot $target
echo "******************** Step6 ----- > clone_snap $sourceSnapshot $target"
if clone_snap $sourceSnapshot $target; then
   echo "clone_snap $sourceSnapshot $target succeeded."
else
   echo "clone_snap $sourceSnapshot $target may have failed"
   restore_snap $targetSnapshot
   delete_snap $targetSnapshot
   delete_snap $sourceSnapshot
   enable_table $source 
   exit
fi

# Step7: delete_snap $sourceSnapshot
echo "******************** Step7 ----- > delete_snap $sourceSnapshot"
if delete_snap $sourceSnapshot; then
   echo "delete_snap $sourceSnapshot succee ded."
else
   echo "delete_snap $sourceSnapshot may have failed"
fi

# Step8: delete_snap $sourceSnapshot
echo "******************** Step8 ----- > delete_snap $sourceSnapshot"
if enable_table $source; then
   echo "enable_table $source succeeded."
else
   echo "enable_table $source may have failed"
   exit
fi
