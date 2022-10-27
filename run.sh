#/bin/sh

for f in $(find $1 -name "*.py")
do
    python3 -m libcst.tool codemod convert_solid_to_op.ConvertSolidToOp $f && \
    python3 -m libcst.tool codemod convert_pipeline_to_job.ConvertPipelineToJob $f && \
    python3 -m libcst.tool codemod convert_composite_to_graph.ConvertCompositeToGraph $f && \
    python3 -m libcst.tool codemod execute_pipeline_to_in_process.ConvertExecutePipeline $f
    if [[ $f == *"test"*".py" ]];then
        pytest $f -xvv --ff
    fi
    python3 -m autoflake --in-place --remove-all-unused-imports $f || \
    { echo "failed for $f"; exit 1; }
    pylint $f --rcfile=/Users/christopherdecarolis/dagster_2/pyproject.toml
done
