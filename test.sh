ITER_TIMES=10
if [ $# -lt 1 ]; then
    TEST_NAME=2
else
    TEST_NAME=$1
fi

echo $TEST_NAME
LOG_FILENAME="log.out"
last_elapse=0

for ((i=1;i<=$ITER_TIMES;i++))
do
    if [ "$last_elapse" != "0" ];then
        printf "Last iteration elapse = $last_elapse s. "
    fi
    printf "Running Iter $i...\r"
    cargo test $TEST_NAME > $LOG_FILENAME 2>&1
    if [ $? -ne 0 ]; then
        seed=`cat $LOG_FILENAME | grep -Po "(?<=SEED=)[0-9]+" | head -n 1`
        name=`cat $LOG_FILENAME | grep -Po "(?<=[-]{4} )(.*)(?= stdout)" | head -n 1`
        echo "[FAILED] $name failed, TEST_SEED=$seed"
        `RUST_LOG=info MADSIM_TEST_SEED=$seed cargo test $name -- --exact > $LOG_FILENAME 2>&1`
        break
    else
        last_elapse=`cat $LOG_FILENAME | grep -Po "(?<=finished in )(\d+\.\d+)"`
    fi
done

if [ $i -eq `expr $ITER_TIMES + 1` ]; then
    echo "Test $ITER_TIMES times all passed!                                           "
fi