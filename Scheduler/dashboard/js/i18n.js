/* ============================================================================
 * Scheduler Dashboard 国际化（i18n）模块
 * ============================================================================
 * 文件作用:
 *   实现 Dashboard 页面的中英文双语支持，提供翻译字典、翻译函数、
 *   语言切换与下拉菜单交互逻辑。
 *
 * 核心机制:
 *   1. I18N 字典     - 包含 zh / en 两套翻译，值可以是字符串或函数(带参数)
 *   2. data-i18n 属性 - HTML 静态文本通过此属性标记翻译键，applyI18n() 批量更新
 *   3. data-i18n-ph  - 输入框 placeholder 的翻译键
 *   4. t() 函数      - 在 JS 动态生成的 HTML 中调用，获取当前语言的翻译
 *   5. localStorage  - 持久化用户语言偏好 (key: 'scheduler-lang')，默认中文
 *
 * 模块依赖关系:
 *   - 本文件必须先于 dashboard.js 加载（提供 I18N / t / currentLang / setLang 等）
 *   - applyI18n() / setLang() 中会调用 render() / updateJobDropdown()，
 *     这两个函数定义在 dashboard.js 中，故切换语言时需 dashboard.js 已加载
 *
 * 翻译键命名规范:
 *   - 静态文本: 语义化名称，如 tab_dashboard / stat_active_jobs
 *   - 动态消息: t_ 前缀，如 t_task_added / t_job_paused
 *   - 状态值: 直接用状态名作 key，如 running / paused / completed
 * ============================================================================
 */

        // ===== I18N 国际化字典 =====
        // zh: 中文翻译（默认语言）
        // en: 英文翻译
        // 值为字符串: 直接返回；值为函数: 调用时传入参数生成动态文本
        const I18N = {
            zh: {
                tab_dashboard: '仪表盘', tab_history: '历史', tab_failed: '失败',
                add_task: '添加任务', running: '运行中', stopped: '已停止',
                connecting: '连接中', live: '已连接', offline: '离线',
                stat_active_jobs: '活跃任务', stat_total_runs: '总执行次数',
                stat_failures: '失败次数', stat_uptime: '运行时长',
                scheduled_jobs: '计划任务', execution_history: '执行历史',
                failed_jobs_dlq: '失败任务（死信队列）', clear_all: '清空全部',
                dlq_desc: '已耗尽所有重试次数的任务会记录在此。用于诊断重复问题和追踪任务失败。',
                th_job: '任务', th_schedule: '调度', th_status: '状态',
                th_last_runs: '最近5次', th_next_run: '下次执行', th_actions: '操作',
                th_time: '时间', th_duration: '耗时', th_details: '详情',
                th_error: '错误', th_run_no: '执行#', th_retries: '重试',
                all_statuses: '所有状态', completed: '已完成', failed: '已失败',
                missed: '已错过', all_jobs: '所有任务',
                paused: '已暂停', scheduled: '已计划', running: '执行中',
                resume: '恢复', pause: '暂停', cancel: '取消',
                run_now: '立即执行', view_details: '查看详情',
                edit_task: '修改任务', edit_task_title: '修改任务',
                save: '保存', saving: '保存中...',
                job_detail: '任务详情', execution_detail: '执行详情', d_job_id: '任务 ID',
                d_func_name: '函数名', d_status: '状态',
                d_schedule: '调度', d_next_run: '下次执行',
                d_countdown: '倒计时', d_run_count: '执行次数',
                d_retry_count: '重试次数', d_max_retries: '最大重试次数', d_timeout: '超时（秒）',
                d_last_run: '上次执行', d_paused: '是否暂停',
                d_script_path: '脚本路径', d_script_args: '脚本参数',
                d_script_code: '内联代码',
                d_yes: '是', d_no: '否', d_none: '无',
                t_confirm_run: '确定立即执行此任务吗？',
                t_job_not_found: '任务不存在', t_load_detail_failed: '加载详情失败',
                no_jobs: '暂无计划任务', no_history: '暂无历史记录',
                no_dead_letters: '死信队列中暂无失败任务',
                entries: '条记录', job_singular: '个任务', job_plural: '个任务',
                per_page: '每页:', query: '查询', reset: '重置',
                run_no: '执行 #', success_rate: '% 成功率', unknown_error: '未知错误',
                no_data: '无数据', now: '现在',
                add_new_task: '添加新任务', task_source: '任务来源',
                script_file: '脚本文件', inline_code: '内联代码',
                script_path: '脚本路径', python_code: 'Python 代码',
                hint_script_path: '要执行的 Python 脚本文件的绝对路径。',
                hint_python_code: '要执行的 Python 代码。将写入临时文件并使用项目的 Python 解释器运行。',
                task_name: '任务名称', schedule_type: '调度类型',
                sch_interval: '间隔执行（每 N 秒）', sch_daily: '每日（指定时间）',
                sch_cron: 'Cron 表达式', sch_once: '一次（一次性延迟）',
                interval_seconds: '间隔（秒）', time_hhmm: '时间（HH:MM）',
                cron_expression: 'Cron 表达式', delay_seconds: '延迟（秒）',
                hint_interval: '执行间隔（秒）。如 60 = 每分钟一次。',
                hint_daily_time: '每天执行的时间。',
                hint_cron: '格式：分 时 日 月 周。如 "*/30 * * * *" = 每 30 分钟。',
                cron_builder_title: '可视化配置（自动生成表达式）',
                cron_minute: '分钟 (0-59)', cron_hour: '小时 (0-23)', cron_day: '日 (1-31)',
                cron_month: '月 (1-12)', cron_week: '周 (0-6, 0=周日)',
                cron_mode_every: '每分钟 (*)', cron_mode_step: '每 N 分钟', cron_mode_specific: '指定分钟', cron_mode_range: '范围',
                cron_mode_every_h: '每小时 (*)', cron_mode_step_h: '每 N 小时', cron_mode_specific_h: '指定小时', cron_mode_range_h: '范围',
                cron_mode_every_d: '每天 (*)', cron_mode_step_d: '每 N 天', cron_mode_specific_d: '指定日', cron_mode_range_d: '范围',
                cron_mode_every_m: '每月 (*)', cron_mode_step_m: '每 N 月', cron_mode_specific_m: '指定月', cron_mode_range_m: '范围',
                cron_mode_every_w: '每天 (*)', cron_mode_specific_w: '指定星期', cron_mode_range_w: '范围',
                cron_presets: '常用预设：', cron_preset_min: '每分钟', cron_preset_5min: '每5分钟',
                cron_preset_hourly: '每小时', cron_preset_daily9: '每天9点', cron_preset_weekday9: '工作日9点',
                cron_preset_weekly: '每周日0点', cron_preset_monthly: '每月1号0点',
                cron_preview: '生成表达式：',
                hint_once_delay: '一次性执行前的延迟秒数。',
                script_arguments: '脚本参数',
                hint_script_args: '传给脚本的命令行参数。无则留空。',
                max_retries: '最大重试', timeout_s: '超时（秒）', timezone: '时区',
                tz_local: 'local (系统本地时区)',
                catch_up: '补偿错过的执行',
                hint_catch_up: '勾选后，调度器停机期间错过的执行将被补偿。',
                cancel: '取消', adding: '添加中...',
                ph_search: '搜索...', ph_search_failed: '搜索失败任务...',
                ph_search_job: '搜索任务名称...',
                ph_task_name: "自动生成（脚本文件名或 'inline_code'）",
                ph_script_args: '--debug --port 8080（空格分隔）',
                ph_none: '无', ph_local: '本地',
                // Toast / 动态消息
                t_task_added: id => `任务已添加（ID: ${id}）`,
                t_add_failed: '添加任务失败', t_request_failed: msg => `请求失败：${msg}`,
                t_job_name_exists: name => `任务名称「${name}」已存在，请更换名称或先取消同名任务`,
                t_script_path_required: '脚本路径为必填项', t_code_required: 'Python 代码为必填项',
                t_interval_required: '请输入有效的间隔（秒）', t_daily_time_required: '每日调度时间为必填项',
                t_cron_required: 'Cron 表达式为必填项', t_delay_required: '请输入有效的延迟（秒）',
                t_job_paused: '任务已暂停', t_job_resumed: '任务已恢复',
                t_job_cancelled: '任务已取消', t_job_triggered: '任务已触发',
                t_task_updated: '任务已更新', t_update_failed: '更新任务失败',
                t_job_not_paused: '仅可修改已暂停的任务，请先暂停后再编辑',
                t_confirm_cancel: '确定取消此任务吗？',
                t_confirm_pause: '确定暂停此任务吗？',
                t_confirm_resume: '确定恢复此任务吗？',
                t_confirm_clear_dlq: '确定清空死信队列中的所有失败任务记录吗？',
                t_cleared: n => `已清除 ${n} 条记录`, t_clear_failed: '清除失败',
                t_completed: name => `${name} 已完成`, t_failed: name => `${name} 已失败`,
            },
            en: {
                tab_dashboard: 'Dashboard', tab_history: 'History', tab_failed: 'Failed',
                add_task: 'Add Task', running: 'Running', stopped: 'Stopped',
                connecting: 'Connecting', live: 'Live', offline: 'Offline',
                stat_active_jobs: 'Active Jobs', stat_total_runs: 'Total Runs',
                stat_failures: 'Failures', stat_uptime: 'Uptime',
                scheduled_jobs: 'Scheduled Jobs', execution_history: 'Execution History',
                failed_jobs_dlq: 'Failed Jobs (Dead Letter Queue)', clear_all: 'Clear All',
                dlq_desc: 'Jobs that have exhausted all retry attempts are logged here. Use this to diagnose recurring issues and track job failures.',
                th_job: 'Job', th_schedule: 'Schedule', th_status: 'Status',
                th_last_runs: 'Last 5 Runs', th_next_run: 'Next Run', th_actions: 'Actions',
                th_time: 'Time', th_duration: 'Duration', th_details: 'Details',
                th_error: 'Error', th_run_no: 'Run #', th_retries: 'Retries',
                all_statuses: 'All Statuses', completed: 'Completed', failed: 'Failed',
                missed: 'Missed', all_jobs: 'All Jobs',
                paused: 'Paused', scheduled: 'Scheduled', running: 'Running',
                resume: 'Resume', pause: 'Pause', cancel: 'Cancel',
                run_now: 'Run Now', view_details: 'Details',
                edit_task: 'Edit', edit_task_title: 'Edit Task',
                save: 'Save', saving: 'Saving...',
                job_detail: 'Job Detail', execution_detail: 'Execution Detail', d_job_id: 'Job ID',
                d_func_name: 'Function Name', d_status: 'Status',
                d_schedule: 'Schedule', d_next_run: 'Next Run',
                d_countdown: 'Countdown', d_run_count: 'Run Count',
                d_retry_count: 'Retry Count', d_max_retries: 'Max Retries', d_timeout: 'Timeout (s)',
                d_last_run: 'Last Run', d_paused: 'Paused',
                d_script_path: 'Script Path', d_script_args: 'Script Arguments',
                d_script_code: 'Inline Code',
                d_yes: 'Yes', d_no: 'No', d_none: 'None',
                t_confirm_run: 'Run this job now?',
                t_job_not_found: 'Job not found', t_load_detail_failed: 'Failed to load detail',
                no_jobs: 'No scheduled jobs', no_history: 'No history entries',
                no_dead_letters: 'No failed jobs in dead letter queue',
                entries: 'entries', job_singular: 'job', job_plural: 'jobs',
                per_page: 'Per page:', query: 'Query', reset: 'Reset',
                run_no: 'Run #', success_rate: '% success', unknown_error: 'Unknown error',
                no_data: 'No data', now: 'Now',
                add_new_task: 'Add New Task', task_source: 'Task Source',
                script_file: 'Script File', inline_code: 'Inline Code',
                script_path: 'Script Path', python_code: 'Python Code',
                hint_script_path: 'Absolute path to the Python script file to execute.',
                hint_python_code: "Python code to execute. Will be written to a temp file and run with the project's Python interpreter.",
                task_name: 'Task Name', schedule_type: 'Schedule Type',
                sch_interval: 'Interval (every N seconds)', sch_daily: 'Daily (at specific time)',
                sch_cron: 'Cron Expression', sch_once: 'Once (one-time delay)',
                interval_seconds: 'Interval (seconds)', time_hhmm: 'Time (HH:MM)',
                cron_expression: 'Cron Expression', delay_seconds: 'Delay (seconds)',
                hint_interval: 'Execution interval in seconds. E.g. 60 = every minute.',
                hint_daily_time: 'Time of day to execute.',
                hint_cron: 'Format: minute hour day month weekday. E.g. "*/30 * * * *" = every 30 min.',
                cron_builder_title: 'Visual Builder (auto-generates expression)',
                cron_minute: 'Minute (0-59)', cron_hour: 'Hour (0-23)', cron_day: 'Day (1-31)',
                cron_month: 'Month (1-12)', cron_week: 'Week (0-6, 0=Sun)',
                cron_mode_every: 'Every minute (*)', cron_mode_step: 'Every N minutes', cron_mode_specific: 'Specific minutes', cron_mode_range: 'Range',
                cron_mode_every_h: 'Every hour (*)', cron_mode_step_h: 'Every N hours', cron_mode_specific_h: 'Specific hours', cron_mode_range_h: 'Range',
                cron_mode_every_d: 'Every day (*)', cron_mode_step_d: 'Every N days', cron_mode_specific_d: 'Specific days', cron_mode_range_d: 'Range',
                cron_mode_every_m: 'Every month (*)', cron_mode_step_m: 'Every N months', cron_mode_specific_m: 'Specific months', cron_mode_range_m: 'Range',
                cron_mode_every_w: 'Every day (*)', cron_mode_specific_w: 'Specific weekdays', cron_mode_range_w: 'Range',
                cron_presets: 'Presets:', cron_preset_min: 'Every minute', cron_preset_5min: 'Every 5 min',
                cron_preset_hourly: 'Hourly', cron_preset_daily9: 'Daily 9AM', cron_preset_weekday9: 'Weekdays 9AM',
                cron_preset_weekly: 'Weekly (Sun 0AM)', cron_preset_monthly: 'Monthly (1st 0AM)',
                cron_preview: 'Generated:',
                hint_once_delay: 'Delay before one-time execution in seconds.',
                script_arguments: 'Script Arguments',
                hint_script_args: 'Command-line arguments passed to the script. Leave empty if none.',
                max_retries: 'Max Retries', timeout_s: 'Timeout (s)', timezone: 'Timezone',
                tz_local: 'local (system local timezone)',
                catch_up: 'Catch up missed executions',
                hint_catch_up: 'If checked, executions missed during scheduler downtime will be compensated.',
                cancel: 'Cancel', adding: 'Adding...',
                ph_search: 'Search...', ph_search_failed: 'Search failed jobs...',
                ph_search_job: 'Search job name...',
                ph_task_name: "auto-generated (script filename or 'inline_code')",
                ph_script_args: '--debug --port 8080 (space-separated)',
                ph_none: 'none', ph_local: 'local',
                t_task_added: id => `Task added (ID: ${id})`,
                t_add_failed: 'Failed to add task', t_request_failed: msg => `Request failed: ${msg}`,
                t_job_name_exists: name => `Job name '${name}' already exists. Please use a different name or cancel the existing one first.`,
                t_script_path_required: 'Script path is required', t_code_required: 'Python code is required',
                t_interval_required: 'Valid interval (seconds) is required', t_daily_time_required: 'Daily schedule time is required',
                t_cron_required: 'Cron expression is required', t_delay_required: 'Valid delay (seconds) is required',
                t_job_paused: 'Job paused', t_job_resumed: 'Job resumed',
                t_job_cancelled: 'Job cancelled', t_job_triggered: 'Job triggered',
                t_task_updated: 'Task updated', t_update_failed: 'Failed to update task',
                t_job_not_paused: 'Only paused jobs can be edited. Please pause the job first.',
                t_confirm_cancel: 'Cancel this job?',
                t_confirm_pause: 'Pause this job?',
                t_confirm_resume: 'Resume this job?',
                t_confirm_clear_dlq: 'Clear all failed job entries from the dead letter queue?',
                t_cleared: n => `Cleared ${n} entries`, t_clear_failed: 'Failed to clear',
                t_completed: name => `${name} completed`, t_failed: name => `${name} failed`,
            }
        };

        // 当前语言（从 localStorage 读取 'scheduler-lang'，未设置时默认中文 'zh'）
        // 全局变量，被 dashboard.js 中的渲染函数间接使用（通过 t()）
        let currentLang = localStorage.getItem('scheduler-lang') || 'zh';

        /**
         * 翻译函数：根据 key 从当前语言字典取值，支持带参数的动态文本
         * @param {string} key  - 翻译字典中的键名
         * @param {...any} args - 当字典值为函数时传入的参数（如任务 ID、错误消息）
         * @returns {string} 翻译后的文本；若 key 不存在则返回 key 本身（降级处理）
         *
         * 用法示例:
         *   t('add_task')                  // → '添加任务' / 'Add Task'
         *   t('t_task_added', 'job_1')      // → '任务已添加（ID: job_1）'
         *   t(job.status)                   // → 状态值翻译，如 'running' → '执行中'
         */
        function t(key, ...args) {
            const val = (I18N[currentLang] || I18N.zh)[key];
            if (typeof val === 'function') return val(...args);
            return val !== undefined ? val : key;
        }

        /** 获取当前语言代码（'zh' 或 'en'），供其他模块格式化逻辑使用 */
        function getCurrentLang() {
            return currentLang;
        }

        /**
         * 切换语言并持久化到 localStorage
         * @param {string} lang - 目标语言代码: 'zh' 或 'en'
         *
         * 执行流程:
         *   1. 更新 currentLang 并持久化
         *   2. 更新下拉菜单的激活态与触发器显示
         *   3. applyI18n() 重新翻译所有静态文本
         *   4. render() 重新渲染动态内容（表格、状态等）
         *   5. updateJobDropdown() 重新渲染任务筛选下拉
         *   6. 关闭语言下拉菜单
         */
        function setLang(lang) {
            currentLang = lang;
            localStorage.setItem('scheduler-lang', lang);
            // 更新下拉菜单选项激活态与触发器显示
            updateLangDropdown();
            applyI18n();
            // 重新渲染动态内容
            render();
            updateJobDropdown();
            // 若添加/编辑任务弹窗处于编辑模式，恢复其标题与按钮文案
            if (typeof setAddTaskModalMode === 'function' && typeof editingJobId !== 'undefined' && editingJobId) {
                setAddTaskModalMode(true);
            }
            // 选择后关闭下拉
            closeLangDropdown();
        }

        /**
         * 更新语言下拉菜单的显示状态
         * - 触发器按钮显示当前语言缩写（中 / EN）
         * - 高亮当前选中的语言选项（添加 .active 类）
         */
        function updateLangDropdown() {
            // 触发器显示当前语言缩写
            const cur = document.getElementById('lang-current');
            if (cur) cur.textContent = currentLang === 'zh' ? '中' : 'EN';
            // 选项激活态
            document.querySelectorAll('.lang-option').forEach(opt => {
                opt.classList.toggle('active', opt.getAttribute('data-lang') === currentLang);
            });
        }

        /**
         * 切换语言下拉菜单的展开/收起状态
         * @param {Event} e - 点击事件，stopPropagation 防止触发 document 的关闭监听
         */
        function toggleLangDropdown(e) {
            e.stopPropagation();
            document.getElementById('lang-dropdown').classList.toggle('open');
        }

        /** 关闭语言下拉菜单（移除 .open 类） */
        function closeLangDropdown() {
            document.getElementById('lang-dropdown').classList.remove('open');
        }

        // 全局点击监听：点击下拉菜单外部时自动关闭
        // 通过 contains() 判断点击目标是否在下拉菜单内，外部点击则关闭
        document.addEventListener('click', (e) => {
            const dropdown = document.getElementById('lang-dropdown');
            if (dropdown && !dropdown.contains(e.target)) {
                closeLangDropdown();
            }
        });

        /**
         * 将所有 data-i18n / data-i18n-ph 属性应用到 DOM 元素
         *
         * 遍历策略:
         *   - [data-i18n]:      设置元素文本内容为翻译值
         *   - [data-i18n-ph]:   设置元素 placeholder 属性为翻译值
         *
         * 特殊处理:
         *   若元素内部包含 <span class="required">*</span>（表单必填标记），
         *   设置 textContent 会清空子节点，故需先保存 required 元素再重新追加。
         *
         * 注意:
         *   此函数只处理静态文本。动态生成的 HTML（如表格行）应在
         *   生成时直接调用 t() 嵌入翻译值。
         */
        function applyI18n() {
            document.querySelectorAll('[data-i18n]').forEach(el => {
                const key = el.getAttribute('data-i18n');
                const val = t(key);
                // 保留子元素中的 <span class="required">*</span> 等
                const required = el.querySelector('.required');
                if (required) {
                    el.textContent = val + ' ';
                    el.appendChild(required);
                } else {
                    el.textContent = val;
                }
            });
            document.querySelectorAll('[data-i18n-ph]').forEach(el => {
                el.setAttribute('placeholder', t(el.getAttribute('data-i18n-ph')));
            });
        }
