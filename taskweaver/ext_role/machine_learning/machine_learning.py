import os
from typing import Dict, Literal, Optional, List

from injector import inject
from taskweaver.code_interpreter.code_interpreter.code_interpreter import (
    update_verification,
    update_execution,
)

from taskweaver.code_interpreter.code_interpreter.code_generator import format_code_feedback

from taskweaver.code_interpreter.code_executor import CodeExecutor
from taskweaver.code_interpreter.code_interpreter import (
    CodeGenerator,
    format_code_revision_message,
    format_output_revision_message,
)
from taskweaver.code_interpreter.code_verification import code_snippet_verification, format_code_correction_message
from taskweaver.code_interpreter.interpreter import Interpreter
from taskweaver.logging import TelemetryLogger
from taskweaver.memory import Memory, Post, Round
from taskweaver.memory.attachment import AttachmentType, Attachment
from taskweaver.memory.experience import Experience, ExperienceGenerator
from taskweaver.memory.plugin import PluginEntry, PluginRegistry
from taskweaver.module.event_emitter import PostEventProxy, SessionEventEmitter
from taskweaver.module.tracing import Tracing, get_tracer, tracing_decorator
from taskweaver.role import Role
from taskweaver.role.role import RoleConfig, RoleEntry
from taskweaver.llm.util import ChatMessageType, format_chat_message
from taskweaver.utils import read_yaml


class MachineLearningConfig(RoleConfig):
    def _configure(self):

        self.prompt_file_path = self._get_str(
            "config_file_path",
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "query_data_prompt.yaml",
            ),
        )
        self.use_local_uri = self._get_bool(
            "use_local_uri",
            self.src.get_bool(
                "use_local_uri",
                True,
            ),
        )
        self.max_retry_count = self._get_int("max_retry_count", 3)

        # for verification
        self.code_verification_on = self._get_bool("code_verification_on", False)
        self.prompt_compression = self._get_bool("prompt_compression", False)
        self.allowed_modules = self._get_list(
            "allowed_modules",
            [
                "pandas",
                "matplotlib",
                "numpy",
                "sklearn",
                "scipy",
                "seaborn",
                "datetime",
                "typing",
            ],
        )
        self.blocked_functions = self._get_list(
            "blocked_functions",
            [
                "eval",
                "exec",
                "execfile",
                "compile",
                "open",
                "input",
                "raw_input",
                "reload",
                "__import__",
            ],
        )
        self.use_experience = self._get_bool("use_experience", False)


class MachineLearning(Role, Interpreter):
    @inject
    def __init__(
        self,
        generator: CodeGenerator,
        executor: CodeExecutor,
        logger: TelemetryLogger,
        tracing: Tracing,
        event_emitter: SessionEventEmitter,
        config: MachineLearningConfig,
        role_entry: RoleEntry,
    ):
        super().__init__(config, logger, tracing, event_emitter, role_entry)
        self.role_name = "QueryData"
        self.generator = generator
        self.generator.set_alias(self.alias)
        self.generator.configure_verification(
            code_verification_on=self.config.code_verification_on,
            allowed_modules=self.config.allowed_modules,
            blocked_functions=self.config.blocked_functions,
        )
        self.prompt_data = read_yaml(self.config.prompt_file_path)
        self.instruction = self.prompt_data["content"]
        self.conversation_head_template = self.prompt_data["conversation_head"]
        self.user_message_head_template = self.prompt_data["user_message_head"]
        self.conversation_record = self.prompt_data["conversation_record"]
        self.executor = executor
        self.logger = logger
        self.tracing = tracing
        self.event_emitter = event_emitter
        self.retry_count = 0
        self.plugin_pool = self.generator.get_plugin_pool()

        self.logger.info(f"{self.alias} initialized successfully.")
    
    def update_session_variables(self, session_variables: Dict[str, str]):
        self.logger.info(f"Updating session variables: {session_variables}")
        self.executor.update_session_var(session_variables)

    def compose_verification_requirements(
            self,
    ) -> str:
        requirements: List[str] = []
        if not self.code_verification_on:
            return ""

        if len(self.config.allowed_modules) > 0:
            requirements.append(
                f"- {self.role_name} can only import the following Python modules: "
                + ", ".join([f"{module}" for module in self.config.allowed_modules]),
            )

        if len(self.allowed_modules) == 0:
            requirements.append(f"- {self.role_name} cannot import any Python modules.")

        if len(self.blocked_functions) > 0:
            requirements.append(
                f"- {self.role_name} cannot use the following Python functions: "
                + ", ".join([f"{function}" for function in self.blocked_functions]),
            )
        return "\n".join(requirements)

    def compose_prompt(
            self,
            rounds: List[Round],
            plugins: List[PluginEntry],
            selected_experiences: Optional[List[Experience]] = None,
    ) -> List[ChatMessageType]:
        experiences = (
            self.experience_generator.format_experience_in_prompt(
                self.prompt_data["experience_instruction"],
                selected_experiences,
            )
            if self.config.use_experience
            else ""
        )

        chat_history = [format_chat_message(role="system", message=f"{self.instruction}\n{experiences}")]

        summary = None
        if self.config.prompt_compression:
            summary, rounds = self.round_compressor.compress_rounds(
                rounds,
                rounds_formatter=lambda _rounds: str(
                    self.compose_conversation(_rounds, plugins, add_requirements=False),
                ),
                prompt_template=self.compression_template,
            )

        chat_history.extend(
            self.compose_conversation(
                rounds,
                add_requirements=True,
                summary=summary,
                plugins=plugins,
            ),
        )
        return chat_history

    def format_attachment(self, attachment: Attachment):
        if attachment.type == AttachmentType.thought:
            return attachment.content.format(ROLE_NAME=self.role_name)
        else:
            return attachment.content

    def compose_conversation(
            self,
            rounds: List[Round],
            plugins: List[PluginEntry],
            add_requirements: bool = False,
            summary: Optional[str] = None,
    ) -> List[ChatMessageType]:
        cur_round = rounds[-1]
        chat_history: List[ChatMessageType] = []
        ignored_types = [
            AttachmentType.revise_message,
            AttachmentType.verification,
            AttachmentType.code_error,
            AttachmentType.execution_status,
            AttachmentType.execution_result,
        ]

        is_first_post = True
        last_post: Post = None
        for round_index, conversation_round in enumerate(rounds):
            for post_index, post in enumerate(conversation_round.post_list):
                # compose user query
                user_message = ""
                assistant_message = ""
                is_final_post = round_index == len(rounds) - 1 and post_index == len(conversation_round.post_list) - 1

                if post.send_from == "Planner" and post.send_to == self.alias:
                    # to avoid planner imitating the below handcrafted format,
                    # we merge plan and query message in the code generator here
                    user_query = conversation_round.user_query
                    enrichment = f"The user request is: {user_query}\n\n"

                    supplementary_info_dict = cur_round.read_board()
                    supplementary_info = "\n".join([bulletin for bulletin in supplementary_info_dict.values()])
                    if supplementary_info != "":
                        enrichment += (
                            f"To better understand the user request, here is some additional information:\n"
                            f" {supplementary_info}\n\n"
                        )

                    user_feedback = "None"
                    if last_post is not None and last_post.send_from == self.alias:
                        user_feedback = format_code_feedback(last_post)

                    user_message += self.user_message_head_template.format(
                        FEEDBACK=user_feedback,
                        MESSAGE=f"{post.message}",
                    )
                elif post.send_from == post.send_to == self.alias:
                    # for code correction
                    user_message += self.user_message_head_template.format(
                        FEEDBACK=format_code_feedback(post),
                        MESSAGE=f"{post.get_attachment(AttachmentType.revise_message)[0]}",
                    )

                    assistant_message = self.post_translator.post_to_raw_text(
                        post=post,
                        content_formatter=self.format_attachment,
                        if_format_message=False,
                        if_format_send_to=False,
                        ignored_types=ignored_types,
                    )
                elif post.send_from == self.alias and (post.send_to == "Planner" or post.send_to == "User"):
                    if is_final_post:
                        # This user message is added to make the conversation complete
                        # It is used to make sure the last assistant message has a feedback
                        # This is only used for examples or context summarization
                        user_message += self.user_message_head_template.format(
                            FEEDBACK=format_code_feedback(post),
                            MESSAGE="This is the feedback.",
                        )

                    assistant_message = self.post_translator.post_to_raw_text(
                        post=post,
                        content_formatter=self.format_attachment,
                        if_format_message=False,
                        if_format_send_to=False,
                        ignored_types=ignored_types,
                    )
                else:
                    raise ValueError(f"Invalid post: {post}")
                last_post = post

                if len(assistant_message) > 0:
                    chat_history.append(
                        format_chat_message(
                            role="assistant",
                            message=assistant_message,
                        ),
                    )
                if len(user_message) > 0:
                    # add requirements to the last user message
                    chat_history.append(
                        format_chat_message(role="user", message=user_message),
                    )

        return chat_history

    def select_plugins_for_prompt(
            self,
            query: str,
    ) -> List[PluginEntry]:
        selected_plugins = self.plugin_selector.plugin_select(
            query,
            self.config.auto_plugin_selection_topk,
        )
        self.selected_plugin_pool.add_selected_plugins(selected_plugins)
        self.logger.info(f"Selected plugins: {[p.name for p in selected_plugins]}")
        self.logger.info(
            f"Selected plugin pool: {[p.name for p in self.selected_plugin_pool.get_plugins()]}",
        )

        return self.selected_plugin_pool.get_plugins()

    @tracing_decorator
    def reply(
        self,
        memory: Memory,
        prompt_log_path: Optional[str] = None,
    ) -> Post:
        post_proxy = self.event_emitter.create_post_proxy(self.alias)
        post_proxy.update_status("generating code")
        rounds = memory.get_role_rounds(
            role=self.alias,
            include_failure_rounds=False,
        )

        # obtain the query from the last round
        query = rounds[-1].post_list[-1].message
        if self.config.use_experience:
            selected_experiences = self.experience_generator.retrieve_experience(query)
        else:
            selected_experiences = None

        chat_history = self.compose_prompt(rounds, self.plugin_pool, selected_experiences)
        self.generator.reply(
            memory,
            post_proxy,
            prompt_log_path,
            chat_history=chat_history
        )

        post_proxy.update_send_to("User")
        if post_proxy.post.message is not None and post_proxy.post.message != "":  # type: ignore
            update_verification(
                post_proxy,
                "NONE",
                "No code verification is performed.",
            )
            update_execution(post_proxy, "NONE", "No code is executed.")

            return post_proxy.end()

        code = next(
            (a for a in post_proxy.post.attachment_list if a.type == AttachmentType.python),
            None,
        )

        if code is None:
            # no code is generated is usually due to the failure of parsing the llm output
            self.tracing.set_span_status("ERROR", "Failed to generate code.")

            update_verification(
                post_proxy,
                "NONE",
                "No code verification is performed.",
            )
            update_execution(
                post_proxy,
                "NONE",
                "No code is executed due to code generation failure.",
            )
            post_proxy.update_message("Failed to generate code.")
            if self.retry_count < self.config.max_retry_count:
                error_message = format_output_revision_message()
                post_proxy.update_attachment(
                    error_message,
                    AttachmentType.revise_message,
                )
                post_proxy.update_send_to("CodeInterpreter")
                self.retry_count += 1
            else:
                self.retry_count = 0

            return post_proxy.end()

        self.tracing.set_span_attribute("code", code.content)
        post_proxy.update_status("verifying code")

        self.tracing.set_span_attribute("code_verification_on", self.config.code_verification_on)
        self.logger.info(f"Code to be verified: {code.content}")
        with get_tracer().start_as_current_span("CodeInterpreter.verify_code") as span:
            span.set_attribute("code", code.content)
            code_verify_errors = code_snippet_verification(
                code.content,
                self.config.code_verification_on,
                allowed_modules=self.config.allowed_modules,
                blocked_functions=self.config.blocked_functions,
            )

        if code_verify_errors is None:
            update_verification(
                post_proxy,
                "NONE",
                "No code verification is performed.",
            )
        elif len(code_verify_errors) > 0:
            self.logger.info(
                f"Code verification finished with {len(code_verify_errors)} errors.",
            )

            code_error = "\n".join(code_verify_errors)
            update_verification(post_proxy, "INCORRECT", code_error)
            post_proxy.update_message(code_error)

            self.tracing.set_span_status("ERROR", "Code verification failed.")
            self.tracing.set_span_attribute("verification_error", code_error)

            if self.retry_count < self.config.max_retry_count:
                post_proxy.update_attachment(
                    format_code_correction_message(),
                    AttachmentType.revise_message,
                )
                post_proxy.update_send_to("CodeInterpreter")
                self.retry_count += 1
            else:
                self.retry_count = 0

            # add execution status and result
            update_execution(
                post_proxy,
                "NONE",
                "No code is executed due to code verification failure.",
            )
            return post_proxy.end()
        elif len(code_verify_errors) == 0:
            update_verification(post_proxy, "CORRECT", "No error is found.")

        post_proxy.update_status("executing code")
        self.logger.info(f"Code to be executed: {code.content}")

        exec_result = self.executor.execute_code(
            exec_id=post_proxy.post.id,
            code=code.content,
        )

        code_output = self.executor.format_code_output(
            exec_result,
            with_code=False,
            use_local_uri=self.config.use_local_uri,
        )

        update_execution(
            post_proxy,
            status="SUCCESS" if exec_result.is_success else "FAILURE",
            result=code_output,
        )

        # add artifact paths
        post_proxy.update_attachment(
            [
                (
                    a.file_name
                    if os.path.isabs(a.file_name) or not self.config.use_local_uri
                    else os.path.join(self.executor.execution_cwd, a.file_name)
                )
                for a in exec_result.artifact
            ],  # type: ignore
            AttachmentType.artifact_paths,
        )

        post_proxy.update_message(
            self.executor.format_code_output(
                exec_result,
                with_code=True,  # the message to be sent to the user should contain the code
                use_local_uri=self.config.use_local_uri,
            ),
            is_end=True,
        )

        if exec_result.is_success or self.retry_count >= self.config.max_retry_count:
            self.retry_count = 0
        else:
            post_proxy.update_send_to("CodeInterpreter")
            post_proxy.update_attachment(
                format_code_revision_message(),
                AttachmentType.revise_message,
            )
            self.retry_count += 1

        if not exec_result.is_success:
            self.tracing.set_span_status("ERROR", "Code execution failed.")

        reply_post = post_proxy.end()

        self.tracing.set_span_attribute("out.from", reply_post.send_from)
        self.tracing.set_span_attribute("out.to", reply_post.send_to)
        self.tracing.set_span_attribute("out.message", reply_post.message)
        self.tracing.set_span_attribute("out.attachments", str(reply_post.attachment_list))

        return reply_post

    def close(self) -> None:
        self.generator.close()
        self.executor.stop()
        super().close()
