ITER_TIMES=20
if [ $# -lt 1 ]; then
    printf "Usage: bash test.sh TEST_NAME\nExample: bash test.sh 3a\n"
    exit 1
else
    TEST_NAME=$1
fi

if [[ "$OSTYPE" =~ ^linux ]]; then
    GREP=grep
elif [[ "$OSTYPE" =~ ^darwin ]]; then
    GREP=ggrep
else
    printf "Do not support os\n"
    exit 1
fi

echo $TEST_NAME
LOG_FILENAME="log.out"
last_elapse=0

# for ((i=1;i<=$ITER_TIMES;i++))
# do
#     if [ "$last_elapse" != "0" ];then
#         printf "Last iteration elapse = $last_elapse s. "
#     fi
#     printf "Running Iter $i...\r"
#     cargo test $TEST_NAME > $LOG_FILENAME 2>&1
#     if [ $? -ne 0 ]; then
#         seed=`cat $LOG_FILENAME | $GREP -Po "(?<=SEED=)[0-9]+" | head -n 1`
#         name=`cat $LOG_FILENAME | $GREP -Po "(?<=[-]{4} )(.*)(?= stdout)" | head -n 1`
#         echo "[FAILED] $name failed, TEST_SEED=$seed"
#         `RUST_LOG=info MADSIM_TEST_SEED=$seed cargo test $name -- --exact > $LOG_FILENAME 2>&1`
#         break
#     else
#         last_elapse=`cat $LOG_FILENAME | $GREP -Po "(?<=finished in )(\d+\.\d+)"`
#     fi
# done

# if [ $i -eq `expr $ITER_TIMES + 1` ]; then
#     echo "Test $ITER_TIMES times all passed!                                           "
# fi

for ((i=1;i<=$ITER_TIMES;i++))
do
    seed=$[$RANDOM*100000+$RANDOM]
    if [ "$last_elapse" != "0" ];then
        printf "Last iteration elapse = $last_elapse s. "
    fi
    printf "Running Iter $i: MADSIM_TEST_SEED=$seed...\r"
    MADSIM_TEST_SEED=$seed timeout 30s cargo test $TEST_NAME > $LOG_FILENAME 2>&1
    code=$?
    if [ $code -ne 0 ]; then
        name=`cat $LOG_FILENAME | $GREP -Po "(?<=[-]{4} )(.*)(?= stdout)" | head -n 1`
        if [ $code -eq 124 ]; then
            echo "[FAILED] $name timeout for 30 seconds, MADSIM_TEST_SEED=$seed"
        else
            echo "[FAILED] $name failed, MADSIM_TEST_SEED=$seed"
            RUST_LOG=info MADSIM_TEST_SEED=$seed timeout 30s cargo test $name -- --exact > $LOG_FILENAME 2>&1
        fi
        break
    else
        last_elapse=`cat $LOG_FILENAME | $GREP -Po "(?<=finished in )(\d+\.\d+)"`
    fi
done

if [ $i -eq `expr $ITER_TIMES + 1` ]; then
    printf "\nTest $ITER_TIMES times all passed!\n"
fi