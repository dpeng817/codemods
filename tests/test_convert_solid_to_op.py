from libcst.codemod import CodemodTest
from codemods.convert_solid_to_op import ConvertSolidToOp


class TestConvertSolidToOp(CodemodTest):

    # The codemod that will be instantiated for us in assertCodemod.
    TRANSFORM = ConvertSolidToOp

    def test_noop(self) -> None:
        before = """
        """

        after = """
        """

        self.assertCodemod(
            before,
            after,
        )

    def test_solid_variations(self) -> None:
        before = """
            @solid
            def the_solid():
                pass

            some_val = "b"

            @solid(
                name="blah",
                input_defs=[InputDefinition("a"), InputDefinition(name=some_val)],
                output_defs=[OutputDefinition(str, some_val), OutputDefinition(str, name="a")],
                required_resource_keys={"foo"}
            )
            def other_solid():
                pass

            @lambda_solid
            def the_lambda_solid():
                pass

            @lambda_solid(output_def=OutputDefinition(str, some_val))
            def the_other_lambda_solid():
                pass

        """
        after = """
            from dagster import In, Out, op

            @op
            def the_op():
                pass

            some_val = "b"

            @op(
                name="blah",
                ins={"a": In(), some_val: In()},
                out={some_val: Out(str, ), "a": Out(str, )},
                required_resource_keys={"foo"}
            )
            def other_op():
                pass

            @op
            def the_op():
                pass

            @op(out={some_val: Out(str, )})
            def the_other_op():
                pass

        """

        self.assertCodemod(
            before,
            after,
        )
