from libcst.codemod import CodemodTest
from codemods.convert_pipeline_to_job import ConvertPipelineToJob


class TestConvertPipelineToJob(CodemodTest):

    # The codemod that will be instantiated for us in assertCodemod.
    TRANSFORM = ConvertPipelineToJob

    def test_noop(self) -> None:
        before = """
        """

        after = """
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_bare_decorator(self) -> None:
        before = """
            @pipeline
            def the_pipeline():
                pass

            execute_pipeline(the_pipeline)
        """
        after = """
            from dagster import job

            @job
            def the_job():
                pass

            execute_pipeline(the_job)
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_call_decorator_mode(self) -> None:
        before = """
            @pipeline(
                mode_defs=[
                    ModeDefinition(
                        name="fakemode",
                        resource_defs={
                            "fake": IOManagerDefinition.hardcoded_io_manager(VersionedInMemoryIOManager()),
                        },
                    ),
                ],
                tags={MEMOIZED_RUN_TAG: "true"},
            )
            def wrap_pipeline():
                wrap()

            execute_pipeline(wrap_pipeline)
        """
        after = """
            from dagster import job

            @job(
                resource_defs={
                        "fake": IOManagerDefinition.hardcoded_io_manager(VersionedInMemoryIOManager()),
                    },
                tags={MEMOIZED_RUN_TAG: "true"},
            )
            def wrap_job():
                wrap()

            execute_pipeline(wrap_job)
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_klass(self) -> None:
        before = """
            PipelineDefinition(
                solid_defs=[the_solid],
                mode_defs=[ModeDefinition("hello", resource_defs={"a": the_resource})],
            )
            """

        after = """
            GraphDefinition(
                node_defs=[the_solid],
            ).to_job(resource_defs={"a": the_resource},)
            """

        self.assertCodemod(
            before,
            after,
        )
