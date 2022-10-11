from libcst.codemod import CodemodTest
from codemods.execute_pipeline_to_in_process import ConvertExecutePipeline


class TestExecutePipeline(CodemodTest):

    # The codemod that will be instantiated for us in assertCodemod.
    TRANSFORM = ConvertExecutePipeline

    def test_noop(self) -> None:
        before = """
        """

        after = """
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_execute_pipeline(self) -> None:
        before = """
            result = execute_pipeline(the_pipeline, instance=the_instance, run_config={"a": "b"})
        """
        after = """
            result = the_pipeline.execute_in_process(instance=the_instance, run_config={"a": "b"})
        """

        self.assertCodemod(
            before,
            after,
        )
