from libcst.codemod import CodemodTest
from codemods.codemod_solid import CodemodSolid


class TestInputOutputDefsChange(CodemodTest):

    # The codemod that will be instantiated for us in assertCodemod.
    TRANSFORM = CodemodSolid

    def test_noop(self) -> None:
        before = """
            @solid
            def the_solid():
                pass
        """
        after = """
            from dagster import op

            @op
            def the_op():
                pass
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_substitution_input_only(self) -> None:
        before = """
            @solid(input_defs=[InputDefinition("hi", dagster_type=str)])
            def the_solid(context):
                pass

            @solid(input_defs=[InputDefinition(dagster_type=str, name="hi")])
            def the_solid():
                pass
        """
        after = """
            from dagster import In, op

            @op(ins = {"hi": In(dagster_type=str)})
            def the_op(context):
                pass

            @op(ins = {"hi": In(dagster_type=str, )})
            def the_op():
                pass
        """

        self.assertCodemod(before, after)

    def test_substitution_output_only(self) -> None:
        before = """
            @solid(output_defs=[OutputDefinition(dagster_type=str)])
            def the_solid():
                pass

            @solid(output_defs=[OutputDefinition(dagster_type=str, name="foo"), OutputDefinition(dagster_type=str, name="bar")])
            def the_solid():
                pass

            @solid(output_defs=[DynamicOutputDefinition(dagster_type=str, name="foo"), OutputDefinition(dagster_type=str, name="bar")])
            def the_solid():
                pass
        """
        after = """
            from dagster import DynamicOut, Out, op

            @op(out = Out(dagster_type=str))
            def the_op():
                pass

            @op(out = {"foo": Out(dagster_type=str, ), "bar": Out(dagster_type=str, )})
            def the_op():
                pass

            @op(out = {"foo": DynamicOut(dagster_type=str, ), "bar": Out(dagster_type=str, )})
            def the_op():
                pass
        """

        self.assertCodemod(before, after)

    def test_output_substitution(self) -> None:
        before = """
            @solid(output_defs=[OutputDefinition(str)])
            def the_solid():
                pass

            @solid(output_defs=[OutputDefinition(str, "foo")])
            def the_solid():
                pass
        """
        after = """
            from dagster import Out, op

            @op(out = Out(str))
            def the_op():
                pass

            @op(out = {"foo": Out(str, )})
            def the_op():
                pass
        """

        self.assertCodemod(before, after)

    def test_substitution_actual(self) -> None:
        before = """
        @solid(version="foo")
        def my_solid():
            return 5

        @pipeline
        def the_pipeline():
            my_solid()

        run_config = {"solids": "my_solid"}

        r"this string contains my_solid"
        """
        after = """
        from dagster import op

        @op(version="foo")
        def my_op():
            return 5

        @pipeline
        def the_pipeline():
            my_op()

        run_config = {"solids": "my_op"}

        r"this string contains my_op"
        """

        self.assertCodemod(before, after)
