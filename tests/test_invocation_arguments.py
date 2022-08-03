from libcst.codemod import CodemodTest
from codemods.invocation_arguments import SwitchInvocationArgumentsCommand


class TestSwitchInvocationArgumentsCommand(CodemodTest):

    # The codemod that will be instantiated for us in assertCodemod.
    TRANSFORM = SwitchInvocationArgumentsCommand

    def test_noop(self) -> None:
        before = """
            ScheduleDefinition(a, b, c)
        """
        after = """
            ScheduleDefinition(a, b, c)
        """

        self.assertCodemod(
            before,
            after,
            symbol="ScheduleDefinition",
            original_arg="foo",
            original_arg_position=2,
            replacement_arg="baz",
        )

    def test_noop_wrong_class(self) -> None:
        before = """
            SensorDefinition(a, foo=bar)
        """
        after = """
            SensorDefinition(a, foo=bar)
        """

        self.assertCodemod(
            before,
            after,
            symbol="ScheduleDefinition",
            original_arg="foo",
            original_arg_position=2,
            replacement_arg="baz",
        )

    def test_substitution_class(self) -> None:
        before = """
            ScheduleDefinition(a, b, foo=bar)
        """
        after = """
            ScheduleDefinition(a, b, baz=bar)
        """

        self.assertCodemod(
            before,
            after,
            symbol="ScheduleDefinition",
            original_arg="foo",
            original_arg_position=2,
            replacement_arg="baz",
        )

    def test_substitution_decorator(self) -> None:
        before = """
            @the_decorator(a, b, foo=bar)
            def the_func():
                pass
        """
        after = """
            @the_decorator(a, b, baz=bar)
            def the_func():
                pass
        """

        self.assertCodemod(
            before,
            after,
            symbol="the_decorator",
            original_arg="foo",
            original_arg_position=2,
            replacement_arg="baz",
        )

    def test_substitution_function(self) -> None:
        before = """
            the_func(a, b, foo=bar)
        """
        after = """
            the_func(a, b, baz=bar)
        """

        self.assertCodemod(
            before,
            after,
            symbol="the_func",
            original_arg="foo",
            original_arg_position=2,
            replacement_arg="baz",
        )

    def test_deletion_function(self) -> None:
        before = """
            the_func(a, b, foo=bar)
        """
        after = """
            the_func(a, b, )
        """

        self.assertCodemod(
            before,
            after,
            symbol="the_func",
            original_arg="foo",
            original_arg_position=2,
            replacement_arg=None,
        )

    def test_deletion_positional(self) -> None:
        before = """
            the_func(a, b, foo)
        """
        after = """
            the_func(a, b, )
        """

        self.assertCodemod(
            before,
            after,
            symbol="the_func",
            original_arg="something",
            original_arg_position=2,
            replacement_arg=None,
        )
