from libcst.codemod import CodemodTest
from codemods.convert_composite_to_graph import ConvertCompositeToGraph


class TestConvertCompositeToGraph(CodemodTest):

    # The codemod that will be instantiated for us in assertCodemod.
    TRANSFORM = ConvertCompositeToGraph

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
            @composite_solid
            def the_composite():
                pass

            @composite_solid
            def the_composite_solid():
                pass

            @composite_solid
            def the_solid():
                pass

            [the_composite, the_composite_solid, the_solid]
        """
        after = """
            from dagster import graph

            @graph
            def the_graph():
                pass

            @graph
            def the_graph():
                pass

            @graph
            def the_graph():
                pass

            [the_graph, the_graph, the_graph]
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_call_decorator_input_defs(self) -> None:
        before = """
            @composite_solid(input_defs=[])
            def the_composite():
                pass
        """
        after = """
            @composite_solid(input_defs=[])
            def the_composite():
                pass
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_config_mapping(self) -> None:
        before = """
            @composite_solid(name="blah", config_schema=str, config_fn=lambda x: None)
            def the_composite():
                pass
        """
        after = """
            from dagster import ConfigMapping, graph

            @graph(config = ConfigMapping(config_schema=str, config_fn=lambda x: None), name="blah", )
            def the_graph():
                pass
        """

        self.assertCodemod(
            before,
            after,
        )
