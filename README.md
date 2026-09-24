<div align="center">

![:shell](https://count.getloli.com/@github_monitor_shell?name=github_monitor_shell&theme=minecraft&padding=7&offset=0&align=top&scale=1&pixelated=1&darkmode=auto)


[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![AstrBot](https://img.shields.io/badge/AstrBot-3.4%2B-orange.svg)](https://github.com/Soulter/AstrBot)
[![GitHub](https://img.shields.io/badge/作者-Shell-blue)](https://github.com/1592363624)

</div>

## 指令列表

- **批量撤回**：批量撤回最近的消息
  - 格式：批量撤回 @用户 撤回数量 (撤回指定用户消息)
  - 格式：批量撤回 撤回数量 (倒序撤回消息)

## 静默撤回

在插件配置中关闭 **撤回结果通知**（`enable_recall_notification=false`），保存并重载插件。
批量撤回、按编号撤回、撤回自身、引用撤回完成后将不发送结果通知，也不会转入默认 LLM 回复。
此开关同时关闭撤回失败的结果通知；参数用法、平台限制和权限提示仍然保留。
自动撤回本身不会发送撤回成功通知。

## 指令与 LLM 流程

本分支基于上游 `v1.1.2`（`b3bb6cd`）修复以下问题：

- 通过 OneBot 直接发送回复后同步 AstrBot 的发送状态，避免其他插件的回复再次触发 LLM；发送失败时保留原消息链供框架发送。
- 对 QQ 上框架已匹配的指令（包括其他插件的指令、别名、子指令）禁止默认 LLM 回退。指令处理器正常执行，插件显式发起的 LLM 请求继续有效。
- 本插件指令和引用撤回 fallback 在静默、参数错误等路径也禁止默认 LLM 回退。普通聊天和未匹配的前缀消息保持原行为。

AstrBot 的 `should_call_llm(True)` 实际设置的是“禁止默认 LLM 请求”标记。
它独立于消息结果对象，避免旧版框架 `clear_result()` 清除 `stop_event()` 的状态后再次触发 LLM。
仅修补 `_has_send_oper` 无法覆盖未发送消息的指令。

## 回归验证

运行插件测试（只需 Python 标准库）：

```sh
python3 -m unittest discover -s tests -v
```

如需核对 AstrBot 源码中的实际分发与 LLM 回退方法，克隆源码并获取测试标签：

```sh
git clone --depth 1 https://github.com/AstrBotDevs/AstrBot.git /tmp/astrbot-batchrecall-review
git -C /tmp/astrbot-batchrecall-review fetch --depth 1 origin tag v4.5.0 tag v4.20.0
ASTRBOT_SOURCE=/tmp/astrbot-batchrecall-review python3 -m unittest discover -s tests -v
```

源码回归测试直接加载事件状态、`call_handler`、`StarRequestSubStage` 和 `ProcessStage` 的方法体，
以替身隔离 QQ 网络、LLM 和发送阶段，覆盖其他插件回复/静默指令、显式 LLM 请求、普通聊天及静默撤回。
默认测试 `v4.5.0`、`v4.20.0` 和 `HEAD`，可通过 `ASTRBOT_REFS` 指定逗号分隔的版本。
本次核对的 HEAD 为 `67c74b7`（AstrBot 4.28.1）；这些测试不代替真实 QQ 协议端联调。

## 🐔 联系作者

- **反馈**：欢迎在 [GitHub Issues](https://github.com/1592363624/astrbot_plugin_batchrecall/issues) 提交问题或建议
QQ群:91219736
telegram:[巅峰阁](https://t.me/ShellDFG)
