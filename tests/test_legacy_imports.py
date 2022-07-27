from libcst.codemod import CodemodTest
from codemods.legacy_dagster_imports import LegacyImportCommand


class TestLegacyImportCommand(CodemodTest):

    # The codemod that will be instantiated for us in assertCodemod.
    TRANSFORM = LegacyImportCommand

    def test_noop(self) -> None:
        before = """
            from some_module import pipeline
        """
        after = """
            from some_module import pipeline
        """

        self.assertCodemod(before, after, symbols=["pipeline"])

    def test_substitution(self) -> None:
        before = """
            from dagster import pipeline
        """
        after = """
            from dagster._legacy import pipeline
        """

        self.assertCodemod(before, after, symbols=["pipeline"])

    def test_substitution_multiple_items(self) -> None:
        before = """
            from dagster import ModeDefinition, pipeline
        """
        after = """
            from dagster import ModeDefinition
            from dagster._legacy import pipeline
        """

        self.assertCodemod(before, after, symbols=["pipeline"])

    def test_substitution_multiple_items_enclosed(self) -> None:
        before = """
            from dagster import ModeDefinition, pipeline, repository
        """
        after = """
            from dagster import ModeDefinition, repository
            from dagster._legacy import pipeline
        """

        self.assertCodemod(before, after, symbols=["pipeline"])

    def test_substitution_multiline(self) -> None:
        before = """
            from dagster import (
                ModeDefinition,
                pipeline,
                repository
            )
        """
        after = """
            from dagster import ModeDefinition, repository
            from dagster._legacy import pipeline
        """

        self.assertCodemod(before, after, symbols=["pipeline"])

    def test_substitution_multiline_multiple_symbols(self) -> None:
        before = """
            from dagster import (
                ModeDefinition,
                pipeline,
                repository,
                AssetGroup,
                asset
            )
        """
        after = """
            from dagster import ModeDefinition, repository, asset
            from dagster._legacy import AssetGroup, pipeline
        """

        self.assertCodemod(before, after, symbols=["AssetGroup", "pipeline"])

    def test_substitution_multiline_legacy_already_exists(self) -> None:
        before = """
            from dagster import (
                ModeDefinition,
                PipelineDefinition,
                repository,
                AssetGroup,
                asset
            )
            from dagster._legacy import pipeline
        """
        after = """
            from dagster import repository, asset
            from dagster._legacy import AssetGroup, ModeDefinition, PipelineDefinition, pipeline
        """
        self.assertCodemod(
            before,
            after,
            symbols=["AssetGroup", "pipeline", "PipelineDefinition", "ModeDefinition"],
        )
