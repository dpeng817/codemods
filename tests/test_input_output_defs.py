from libcst.codemod import CodemodTest
from codemods.change_input_output_defs import ChangeInputOutputDefsCommand


class TestInputOutputDefsChange(CodemodTest):

    # The codemod that will be instantiated for us in assertCodemod.
    TRANSFORM = ChangeInputOutputDefsCommand

    def test_noop(self) -> None:
        before = """
            @op(ins={"a": In()}, out=Out())
            def the_op():
                pass
        """
        after = """
            @op(ins={"a": In()}, out=Out())
            def the_op():
                pass
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_substitution_input_only(self) -> None:
        before = """
            @op(input_defs=[InputDefinition("hi", dagster_type=str)])
            def the_op():
                pass

            @op(input_defs=[InputDefinition(dagster_type=str, name="hi")])
            def the_op():
                pass
        """
        after = """
            @op(ins = {"hi": In(dagster_type=str)})
            def the_op():
                pass

            @op(ins = {"hi": In(dagster_type=str, )})
            def the_op():
                pass
        """

        self.assertCodemod(before, after)

    def test_substitution_output_only(self) -> None:
        before = """
            @op(output_defs=[OutputDefinition(dagster_type=str)])
            def the_op():
                pass

            @op(output_defs=[OutputDefinition("foo", dagster_type=str), OutputDefinition(dagster_type=str, name="bar")])
            def the_op():
                pass
        """
        after = """
            @op(out = Out(dagster_type=str))
            def the_op():
                pass

            @op(out = {"foo": Out(dagster_type=str), "bar": Out(dagster_type=str, )})
            def the_op():
                pass
        """

        self.assertCodemod(before, after)
