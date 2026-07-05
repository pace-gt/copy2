# Scenario driver for the copy2 functional test suite. Invoked via `cmake -P`.
#
# Required -D variables:
#   PYTHON      python3 interpreter
#   GEN         path to gentrees.py
#   VERIFY      path to verify.py
#   COPY2       path to the copy2 binary
#   MODE        empty | identical | modified
#   EXTRAS      ON/OFF  -> pass --extras to the generator
#   ALLOW_EXTRA ON/OFF  -> pass --allow-extra to the verifier
#   CHECK_SPARSE ON/OFF -> pass --check-sparse to the verifier
#   FLAGS       extra copy2 flags as a single string (may be empty)
#   WORKDIR     scratch directory for this scenario
#   SEED        generator seed
#   PROFILE     small | medium
#
# Steps: (1) generate src+dest, (2) run copy2 src dest, (3) verify dest==src.
# Any failure aborts with a non-zero status, which CTest reports as a failure.

if(EXISTS "${WORKDIR}")
  file(REMOVE_RECURSE "${WORKDIR}")
endif()
file(MAKE_DIRECTORY "${WORKDIR}")

# ---- 1. generate -----------------------------------------------------------
set(gen_args --mode ${MODE} --out ${WORKDIR} --seed ${SEED} --profile ${PROFILE})
if(EXTRAS)
  list(APPEND gen_args --extras)
endif()

execute_process(COMMAND ${PYTHON} ${GEN} ${gen_args} RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
  message(FATAL_ERROR "tree generation failed (exit ${rc})")
endif()

# ---- 2. run copy2 ----------------------------------------------------------
file(MAKE_DIRECTORY "${WORKDIR}/data")
separate_arguments(flags_list UNIX_COMMAND "${FLAGS}")

execute_process(
  COMMAND ${COPY2}
          --data-dir ${WORKDIR}/data
          --allocator-mem-size 512MB
          --copy-buffer-size 128MB
          --crawlers 4 --transfers 2 --finish-processors 2
          ${flags_list}
          ${WORKDIR}/src ${WORKDIR}/dest
  RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
  message(FATAL_ERROR "copy2 exited with ${rc}")
endif()

# ---- 3. verify -------------------------------------------------------------
set(verify_args --src ${WORKDIR}/src --dest ${WORKDIR}/dest)
if(ALLOW_EXTRA)
  list(APPEND verify_args --allow-extra)
endif()
if(CHECK_SPARSE)
  list(APPEND verify_args --check-sparse)
endif()

execute_process(COMMAND ${PYTHON} ${VERIFY} ${verify_args} RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
  message(FATAL_ERROR "verification failed (exit ${rc}); workdir kept at ${WORKDIR}")
endif()

# success: clean up scratch space
file(REMOVE_RECURSE "${WORKDIR}")
message(STATUS "scenario ${MODE} OK")
