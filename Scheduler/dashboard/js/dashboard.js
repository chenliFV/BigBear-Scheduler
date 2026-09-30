/* ============================================================================
 * Scheduler Dashboard 主脚本
 * ============================================================================
 * 文件作用:
 *   Dashboard 页面的核心业务逻辑，包含状态管理、SSE 实时数据接收、
 *   三个视图(Dashboard/History/Failed)的渲染、任务操作、添加任务弹窗等。
 *
 * 模块依赖:
 *   - i18n.js:  提供 I18N 字典、t() 翻译函数、setLang()、applyI18n()、currentLang
 *   - 必须在 i18n.js 之后加载（DOMContentLoaded 中调用 applyI18n）
 *   - window.SCHEDULER_PREFIX: 由 dashboard.html 内联脚本注入的 API 路由前缀
 *
 * 数据流:
 *   1. connectSSE() 建立 SSE 连接到 /scheduler/events
 *   2. 后端每秒推送一次状态 JSON (jobs/history/stats/dead_letters)
 *   3. onmessage 解析数据 → checkForToasts() 检测新事件 → render() 刷新视图
 *   4. 用户操作(暂停/恢复/取消)通过 fetch 调用 REST API，后端处理后下次推送生效
 *
 * 全局状态变量:
 *   - eventSource:   SSE 连接实例
 *   - data:          最新状态数据快照
 *   - prevHistory:   上一帧历史，用于检测新事件(toast 提示)
 *   - currentView:   当前激活的视图
 *   - historyStatusFilter: 历史视图的状态筛选
 *   - historyJobFilter:    历史视图的任务名筛选
 *   - historySearch:       历史视图的搜索框输入
 *   - failedSearch:        失败视图的搜索框输入
 *   - currentSource:       添加任务弹窗当前的任务源(script-file / inline-code)
 *
 * 主要函数分组:
 *   - 初始化:    DOMContentLoaded / setupListeners
 *   - 视图切换:  switchView
 *   - SSE:       connectSSE / updateConnection / checkForToasts
 *   - 渲染:      render / renderJobs / renderHistory / renderFailed
 *   - 添加任务:  openAddTaskModal / closeAddTaskModal / switchSource /
 *                onScheduleTypeChange / submitAddTask / buildTaskPayload / resetAddTaskForm
 *   - 任务操作:  pauseJob / resumeJob / cancelJob / runJobNow / clearDeadLetters
 *   - 工具:      toast / formatCountdown / formatTime / formatTs / esc
 * ============================================================================
 */


        let eventSource = null;
        let data = { jobs: [], history: [], stats: {}, running: false, dead_letters: [], dead_letter_count: 0 };
        let prevHistory = [];
        let currentView = 'dashboard';
        let historyStatusFilter = 'all';
        let historyJobFilter = 'all';
        let historySearch = '';
        let failedSearch = '';
        // 分页状态
        let historyPageSize = 20;
        let historyPage = 1;
        let failedPageSize = 20;
        let failedPage = 1;
        // 计划任务页面分页/筛选状态
        let jobsStatusFilter = 'all';
        let jobsSearch = '';
        let jobsPageSize = 20;
        let jobsPage = 1;
        // 后端分页查询结果（history/failed 视图数据从 API 获取，不依赖 SSE）
        let historyPageData = { items: [], total: 0 };
        let failedPageData = { items: [], total: 0 };
        // 当前页条目缓存，供详情弹窗按索引取值
        let historyItemsCache = [];
        let failedItemsCache = [];
        // 搜索输入防抖
        let historySearchTimer = null;
        let failedSearchTimer = null;

        const API = window.SCHEDULER_PREFIX;

                /**
         * 页面 DOM 加载完成后的初始化入口
         * 执行顺序: 语言下拉初始化 → 应用翻译 → 绑定事件 → 建立 SSE 连接
         */
        document.addEventListener('DOMContentLoaded', () => {
            // 初始化语言下拉菜单激活态并应用翻译
            updateLangDropdown();
            applyI18n();
            setupListeners();
            connectSSE();
        });

                /**
         * 绑定全局事件监听器
         * - 历史视图的状态/任务名/搜索框筛选
         * - 失败视图的搜索框筛选
         * - 添加任务弹窗的关闭逻辑(仅取消按钮和×号可关闭)
         */
        function setupListeners() {
            // 历史页面：状态下拉、任务名下拉变化时立即查询
            document.getElementById('history-status-filter').addEventListener('change', e => {
                historyStatusFilter = e.target.value;
                historyPage = 1;
                fetchHistory();
            });
            document.getElementById('history-job-filter').addEventListener('change', e => {
                historyJobFilter = e.target.value;
                historyPage = 1;
                fetchHistory();
            });
            // 搜索框：回车或点击查询按钮触发
            document.getElementById('history-search').addEventListener('keydown', e => {
                if (e.key === 'Enter') {
                    historySearch = e.target.value.toLowerCase();
                    historyPage = 1;
                    fetchHistory();
                }
            });
            document.getElementById('history-query-btn').addEventListener('click', () => {
                historySearch = document.getElementById('history-search').value.toLowerCase();
                historyPage = 1;
                fetchHistory();
            });
            // 历史页面：重置按钮清空所有筛选条件
            document.getElementById('history-reset-btn').addEventListener('click', () => {
                document.getElementById('history-search').value = '';
                document.getElementById('history-status-filter').value = 'all';
                document.getElementById('history-job-filter').value = 'all';
                historySearch = '';
                historyStatusFilter = 'all';
                historyJobFilter = 'all';
                historyPage = 1;
                fetchHistory();
            });
            // 失败页面：搜索框回车或点击查询按钮触发
            document.getElementById('failed-search').addEventListener('keydown', e => {
                if (e.key === 'Enter') {
                    failedSearch = e.target.value.toLowerCase();
                    failedPage = 1;
                    fetchFailed();
                }
            });
            document.getElementById('failed-query-btn').addEventListener('click', () => {
                failedSearch = document.getElementById('failed-search').value.toLowerCase();
                failedPage = 1;
                fetchFailed();
            });
            // 失败页面：重置按钮清空搜索条件
            document.getElementById('failed-reset-btn').addEventListener('click', () => {
                document.getElementById('failed-search').value = '';
                failedSearch = '';
                failedPage = 1;
                fetchFailed();
            });
            // 计划任务页面：状态下拉变化时立即筛选
            document.getElementById('jobs-status-filter').addEventListener('change', e => {
                jobsStatusFilter = e.target.value;
                jobsPage = 1;
                renderJobs();
            });
            // 计划任务页面：搜索框回车或点击查询按钮触发
            document.getElementById('jobs-search').addEventListener('keydown', e => {
                if (e.key === 'Enter') {
                    jobsSearch = e.target.value.toLowerCase().trim();
                    jobsPage = 1;
                    renderJobs();
                }
            });
            document.getElementById('jobs-query-btn').addEventListener('click', () => {
                jobsSearch = document.getElementById('jobs-search').value.toLowerCase().trim();
                jobsPage = 1;
                renderJobs();
            });
            // 计划任务页面：重置按钮清空筛选条件
            document.getElementById('jobs-reset-btn').addEventListener('click', () => {
                document.getElementById('jobs-search').value = '';
                document.getElementById('jobs-status-filter').value = 'all';
                jobsSearch = '';
                jobsStatusFilter = 'all';
                jobsPage = 1;
                renderJobs();
            });
            // 初始化可视化 Cron 配置面板
            initCronBuilder();
        }

        /**
         * 初始化可视化 Cron 配置面板
         * 为每个字段的 mode select 和 value input 绑定事件，实时生成 cron 表达式。
         * 同时为预设按钮绑定点击事件。
         */
        function initCronBuilder() {
            const builder = document.querySelector('.cron-builder');
            if (!builder) return;

            // 绑定每个字段的 mode 切换 + value 输入
            builder.querySelectorAll('.cron-mode').forEach(sel => {
                sel.addEventListener('change', () => onCronFieldChange(sel.dataset.field));
            });
            builder.querySelectorAll('.cron-value').forEach(inp => {
                inp.addEventListener('input', () => updateCronExpression());
            });

            // 绑定预设按钮
            builder.querySelectorAll('.cron-preset-btn').forEach(btn => {
                btn.addEventListener('click', () => applyCronPreset(btn.dataset.expr));
            });

            // 初始化生成一次
            updateCronExpression();
        }

        /**
         * 某个字段模式切换时，启用/禁用对应的值输入框并更新表达式
         * @param {string} field - 'minute'|'hour'|'day'|'month'|'week'
         */
        function onCronFieldChange(field) {
            const mode = document.querySelector(`.cron-mode[data-field="${field}"]`).value;
            const valueInput = document.querySelector(`.cron-value[data-field="${field}"]`);
            valueInput.disabled = (mode === 'every');
            updateCronExpression();
        }

        /**
         * 根据所有字段的 mode 和 value 计算 cron 表达式
         * 模式说明：
         *   every    -> 星号 (每)
         *   step     -> 星号/N (每 N 个单位)
         *   specific -> v1,v2,v3 (指定多个值，逗号分隔)
         *   range    -> start-end (范围)
         * @returns {string} 5 段 cron 表达式
         */
        function buildCronFromFields() {
            const fields = ['minute', 'hour', 'day', 'month', 'week'];
            return fields.map(field => {
                const mode = document.querySelector(`.cron-mode[data-field="${field}"]`).value;
                const raw = document.querySelector(`.cron-value[data-field="${field}"]`).value.trim();
                switch (mode) {
                    case 'every': return '*';
                    case 'step': return raw ? `*/${raw}` : '*';
                    case 'specific': return raw || '*';
                    case 'range': return raw || '*';
                    default: return '*';
                }
            }).join(' ');
        }

        /**
         * 更新 cron 表达式显示（输入框 + 预览区）
         */
        function updateCronExpression() {
            const expr = buildCronFromFields();
            document.getElementById('task-cron-expression').value = expr;
            const preview = document.getElementById('cron-preview-text');
            if (preview) preview.textContent = expr;
        }

        /**
         * 应用预设 cron 表达式
         * 将表达式解析回各字段的 mode/value 并更新 UI
         * @param {string} expr - 如 "0 9 * * 1-5"
         */
        function applyCronPreset(expr) {
            const parts = expr.split(/\s+/);
            const fields = ['minute', 'hour', 'day', 'month', 'week'];
            parts.forEach((part, i) => {
                const field = fields[i];
                const modeSel = document.querySelector(`.cron-mode[data-field="${field}"]`);
                const valueInput = document.querySelector(`.cron-value[data-field="${field}"]`);
                let mode, value;
                if (part === '*') {
                    mode = 'every'; value = '';
                } else if (part.startsWith('*/')) {
                    mode = 'step'; value = part.substring(2);
                } else if (part.includes(',')) {
                    mode = 'specific'; value = part;
                } else if (part.includes('-')) {
                    mode = 'range'; value = part;
                } else {
                    // 单个具体值（如 "0"），用 specific 模式
                    mode = 'specific'; value = part;
                }
                // 周字段没有 step 选项，降级为 specific
                if (field === 'week' && mode === 'step') {
                    mode = 'specific'; value = part;
                }
                modeSel.value = mode;
                valueInput.value = value;
                valueInput.disabled = (mode === 'every');
            });
            updateCronExpression();
        }

                /**
         * 切换主视图
         * @param {string} view - 'dashboard' | 'history' | 'failed'
         * 切换标签激活态、视图容器显示，并按需触发对应视图的渲染。
         */
        function switchView(view) {
            currentView = view;
            document.querySelectorAll('.main-tab').forEach((tab, i) => {
                tab.classList.toggle('active',
                    (view === 'dashboard' && i === 0) ||
                    (view === 'history' && i === 1) ||
                    (view === 'failed' && i === 2)
                );
            });
            document.getElementById('dashboard-view').classList.toggle('active', view === 'dashboard');
            document.getElementById('history-view').classList.toggle('active', view === 'history');
            document.getElementById('failed-view').classList.toggle('active', view === 'failed');
            if (view === 'history') fetchHistory();
            if (view === 'failed') fetchFailed();
        }

                /**
         * 建立 SSE (Server-Sent Events) 连接接收实时状态
         * 端点: /scheduler/events
         * 后端每秒推送一次完整状态 JSON，onmessage 中解析并渲染。
         * 连接异常时 onerror 触发，3 秒后自动重连。
         */
        function connectSSE() {
            if (eventSource) eventSource.close();
            eventSource = new EventSource(`${API}/events`);

            eventSource.onopen = () => updateConnection(true);
            eventSource.onerror = () => {
                eventSource.close();
                updateConnection(false);
                setTimeout(connectSSE, 2000);
            };
            eventSource.onmessage = e => {
                try {
                    const newData = JSON.parse(e.data);
                    checkForToasts(newData);
                    data = newData;
                    render();
                } catch (err) {
                    console.error('Parse error:', err);
                }
            };
        }

                /**
         * 更新右上角连接状态指示器
         * @param {boolean} connected - true=已连接(绿) / false=断开(红)
         */
        function updateConnection(connected) {
            const badge = document.getElementById('connection-badge');
            const text = document.getElementById('connection-text');
            badge.classList.toggle('disconnected', !connected);
            text.textContent = connected ? t('live') : t('offline');
        }

                /**
         * 检测新执行事件并弹出 Toast 提示
         * 对比 prevHistory 与 newData.history，找出新增的 completed/failed 事件，
         * 任务完成弹成功提示，失败弹错误提示。
         */
        function checkForToasts(newData) {
            if (prevHistory.length === 0) {
                prevHistory = [...newData.history];
                return;
            }
            const newEvents = newData.history.filter(h =>
                !prevHistory.find(p => p.timestamp === h.timestamp && p.job_id === h.job_id)
            );
            newEvents.forEach(evt => {
                if (evt.status === 'completed') {
                    toast('success', t('t_completed', evt.func_name));
                } else if (evt.status === 'failed') {
                    toast('error', t('t_failed', evt.func_name));
                }
            });
            prevHistory = [...newData.history];
        }

                /**
         * 主渲染函数：根据 data 刷新整个页面
         * 依次更新: 运行状态、统计卡片、失败徽章、清空按钮、当前视图、任务筛选下拉
         */
        function render() {
            // Status
            const statusEl = document.getElementById('scheduler-status');
            statusEl.classList.toggle('stopped', !data.running);
            statusEl.innerHTML = `<span class="dot"></span><span>${data.running ? t('running') : t('stopped')}</span>`;

            // Stats
            document.getElementById('stat-jobs').textContent = data.jobs.length;
            document.getElementById('stat-runs').textContent = (data.stats.total_runs || 0).toLocaleString();
            document.getElementById('stat-failures').textContent = data.stats.total_failures || 0;
            document.getElementById('stat-uptime').textContent = formatUptime(data.stats.uptime_seconds);

            const runs = data.stats.total_runs || 0;
            const fails = data.stats.total_failures || 0;
            const rateEl = document.getElementById('stat-rate');
            if (runs > 0) {
                const rate = ((runs - fails) / runs * 100).toFixed(2);
                rateEl.textContent = `${rate}${t('success_rate')}`;
                rateEl.classList.toggle('bad', parseFloat(rate) < 95);
            } else {
                rateEl.textContent = '';
            }

            // Update failed badge
            const failedBadge = document.getElementById('failed-badge');
            const deadLetterCount = data.dead_letter_count || 0;
            if (deadLetterCount > 0) {
                failedBadge.style.display = 'inline-flex';
                failedBadge.textContent = deadLetterCount > 99 ? '99+' : deadLetterCount;
            } else {
                failedBadge.style.display = 'none';
            }

            // Show/hide clear button
            const clearBtn = document.getElementById('clear-dead-letters-btn');
            clearBtn.style.display = deadLetterCount > 0 ? 'block' : 'none';

            // Render current view
            // history/failed 视图数据由后端分页 API 驱动，不随 SSE 重新渲染
            if (currentView === 'dashboard') {
                renderJobs();
            }

            // Update job filter dropdown
            updateJobDropdown();
        }

                /**
         * 渲染 Dashboard 视图的任务表格
         * 支持按状态、任务名筛选，前端分页。
         * 每行显示: 名称、调度、状态、最近5次运行点、下次执行、操作按钮
         */
        function renderJobs() {
            const tbody = document.getElementById('jobs-tbody');

            // 1. 排序（按任务名）
            let jobs = [...data.jobs].sort((a, b) => a.func_name.localeCompare(b.func_name));

            // 2. 状态筛选
            if (jobsStatusFilter !== 'all') {
                jobs = jobs.filter(j => j.status === jobsStatusFilter);
            }

            // 3. 任务名模糊搜索
            if (jobsSearch) {
                jobs = jobs.filter(j => j.func_name.toLowerCase().includes(jobsSearch));
            }

            const total = jobs.length;

            // 4. 分页
            const totalPages = Math.max(1, Math.ceil(total / jobsPageSize));
            if (jobsPage > totalPages) jobsPage = totalPages;
            const start = (jobsPage - 1) * jobsPageSize;
            const pageJobs = jobs.slice(start, start + jobsPageSize);

            // 更新计数显示
            document.getElementById('jobs-count-text').textContent = `${total} ${t('entries')}`;

            if (total === 0) {
                tbody.innerHTML = `<tr><td colspan="6" class="empty-state">${t('no_jobs')}</td></tr>`;
                renderPagination('jobs-pagination', jobsPage, totalPages, total, jobsPageSize,
                    p => { jobsPage = p; renderJobs(); },
                    'jobs-page-size', jobsPageSize,
                    s => { jobsPageSize = s; jobsPage = 1; renderJobs(); });
                return;
            }

            tbody.innerHTML = pageJobs.map(job => {
                const lastRuns = getLastRuns(job.func_name, 5);
                const runDots = lastRuns.map((r, i) => {
                    const cls = r ? (r.status === 'completed' ? 'success' : 'failed') : '';
                    const tip = r ? `${t(r.status)} - ${formatTime(r.timestamp_readable)}` : t('no_data');
                    return `<div class="run-dot ${cls} run-dot-tooltip" data-tooltip="${esc(tip)}"></div>`;
                }).join('');

                return `
                    <tr>
                        <td><span class="job-name">${esc(job.func_name)}</span></td>
                        <td><span class="job-schedule">${esc(job.schedule)}</span></td>
                        <td><span class="job-status ${job.status}">${t(job.status)}</span></td>
                        <td><div class="run-dots">${runDots}</div></td>
                        <td>
                            <div class="next-run">${job.next_run}</div>
                            <div class="next-run-countdown">${job.paused ? '—' : formatCountdown(job.next_run_in)}</div>
                        </td>
                        <td>
                            <div class="action-btns">
                                <button class="action-btn primary" onclick="runJobNow('${job.job_id}')">${t('run_now')}</button>
                                ${job.status === 'paused'
                        ? `<button class="action-btn" onclick="resumeJob('${job.job_id}')">${t('resume')}</button>`
                        : `<button class="action-btn warning" onclick="pauseJob('${job.job_id}')">${t('pause')}</button>`
                    }
                                <button class="action-btn danger" onclick="cancelJob('${job.job_id}')">${t('cancel')}</button>
                                <button class="action-btn info" onclick="viewJobDetail('${job.job_id}')">${t('view_details')}</button>
                                ${job.status === 'paused'
                        ? `<button class="action-btn edit" onclick="editJob('${job.job_id}')">${t('edit_task')}</button>`
                        : ''
                    }
                            </div>
                        </td>
                    </tr>
                `;
            }).join('');

            renderPagination('jobs-pagination', jobsPage, totalPages, total, jobsPageSize,
                p => { jobsPage = p; renderJobs(); },
                'jobs-page-size', jobsPageSize,
                s => { jobsPageSize = s; jobsPage = 1; renderJobs(); });
        }

                /**
         * 获取指定任务最近 N 次执行记录
         * @param {string} funcName - 任务函数名
         * @param {number} count    - 要获取的记录数
         * @returns {Array} 最近的执行记录数组，不足 count 时用 null 填充
         */
        function getLastRuns(funcName, count) {
            // Get last N completed/failed runs for this job
            const runs = data.history
                .filter(h => h.func_name === funcName && ['completed', 'failed'].includes(h.status))
                .slice(-count);

            // Pad with nulls if less than count
            while (runs.length < count) runs.unshift(null);
            return runs;
        }

                /**
         * 从后端 API 分页获取历史记录。
         * @param {boolean} rerender - 获取完成后是否重新渲染表格
         */
        async function fetchHistory(rerender = true) {
            const params = new URLSearchParams({
                page: historyPage,
                page_size: historyPageSize,
            });
            if (historyStatusFilter !== 'all') params.set('status', historyStatusFilter);
            if (historyJobFilter !== 'all') params.set('func_name', historyJobFilter);
            if (historySearch) params.set('keyword', historySearch);
            try {
                const res = await fetch(`${API}/api/history?${params}`);
                if (res.ok) historyPageData = await res.json();
            } catch (e) { console.error('fetchHistory error:', e); }
            if (rerender) renderHistory();
        }

        /**
         * 渲染 History 视图的历史记录表格
         * 数据来自后端分页 API（historyPageData），支持按状态、任务名、关键词筛选。
         */
        function renderHistory() {
            const { items, total } = historyPageData;
            document.getElementById('history-count').textContent = `${total} ${t('entries')}`;

            const totalPages = Math.max(1, Math.ceil(total / historyPageSize));

            const tbody = document.getElementById('history-tbody');
            if (total === 0) {
                historyItemsCache = [];
                tbody.innerHTML = `<tr><td colspan="5" class="empty-state">${t('no_history')}</td></tr>`;
                renderPagination('history-pagination', historyPage, totalPages, total, historyPageSize,
                    p => { historyPage = p; fetchHistory(); },
                    'history-page-size', historyPageSize,
                    s => { historyPageSize = s; historyPage = 1; fetchHistory(); });
                return;
            }

            historyItemsCache = items;
            tbody.innerHTML = items.map((h, idx) => `
                <tr>
                    <td><span class="history-job">${esc(h.func_name)}</span></td>
                    <td><span class="history-status ${h.status}">${t(h.status)}</span></td>
                    <td><span class="history-time">${h.timestamp_readable || formatTs(h.timestamp)}</span></td>
                    <td><span class="history-duration">${h.execution_time ? h.execution_time.toFixed(2) + 's' : '-'}</span></td>
                    <td><button class="action-btn info" onclick="viewExecutionDetail('history', ${idx})">${t('view_details')}</button></td>
                </tr>
            `).join('');

            renderPagination('history-pagination', historyPage, totalPages, total, historyPageSize,
                p => { historyPage = p; fetchHistory(); },
                'history-page-size', historyPageSize,
                s => { historyPageSize = s; historyPage = 1; fetchHistory(); });
        }

                /**
         * 从后端 API 分页获取死信队列记录。
         * @param {boolean} rerender - 获取完成后是否重新渲染表格
         */
        async function fetchFailed(rerender = true) {
            const params = new URLSearchParams({
                page: failedPage,
                page_size: failedPageSize,
            });
            if (failedSearch) params.set('keyword', failedSearch);
            try {
                const res = await fetch(`${API}/api/dead-letters?${params}`);
                if (res.ok) failedPageData = await res.json();
            } catch (e) { console.error('fetchFailed error:', e); }
            if (rerender) renderFailed();
        }

                /**
         * 渲染 Failed 视图的死信队列表格
         * 数据来自后端分页 API（failedPageData），支持关键词搜索。
         */
        function renderFailed() {
            const { items, total } = failedPageData;
            document.getElementById('failed-count').textContent = `${total} ${t('entries')}`;

            const totalPages = Math.max(1, Math.ceil(total / failedPageSize));

            const tbody = document.getElementById('failed-tbody');
            if (total === 0) {
                failedItemsCache = [];
                tbody.innerHTML = `<tr><td colspan="5" class="empty-state">${t('no_dead_letters')}</td></tr>`;
                renderPagination('failed-pagination', failedPage, totalPages, total, failedPageSize,
                    p => { failedPage = p; fetchFailed(); },
                    'failed-page-size', failedPageSize,
                    s => { failedPageSize = s; failedPage = 1; fetchFailed(); });
                return;
            }

            failedItemsCache = items;
            tbody.innerHTML = items.map((dl, idx) => `
                <tr>
                    <td><span class="history-job">${esc(dl.func_name)}</span></td>
                    <td><span class="history-time">${dl.timestamp_readable || formatTs(dl.timestamp)}</span></td>
                    <td><span style="color:var(--text-secondary)">${dl.retry_count || 0}</span></td>
                    <td><span class="history-duration">${dl.execution_time ? dl.execution_time.toFixed(2) + 's' : '-'}</span></td>
                    <td><button class="action-btn info" onclick="viewExecutionDetail('failed', ${idx})">${t('view_details')}</button></td>
                </tr>
            `).join('');

            renderPagination('failed-pagination', failedPage, totalPages, total, failedPageSize,
                p => { failedPage = p; fetchFailed(); },
                'failed-page-size', failedPageSize,
                s => { failedPageSize = s; failedPage = 1; fetchFailed(); });
        }

        /**
         * 渲染分页控件（右下角，含每页条数下拉框）
         * @param {string} containerId   - 分页容器元素 ID
         * @param {number} current       - 当前页码（从 1 开始）
         * @param {number} totalPages    - 总页数
         * @param {number} total         - 总记录数
         * @param {number} pageSize      - 每页条数
         * @param {function} onPage      - 点击页码时的回调，参数为目标页码
         * @param {string} pageSizeSelectId - 每页条数下拉框的 ID（用于事件绑定）
         * @param {number} currentPageSize - 当前每页条数（用于选中项）
         * @param {function} onPageSizeChange - 每页条数变化时的回调，参数为新条数
         */
        function renderPagination(containerId, current, totalPages, total, pageSize, onPage, pageSizeSelectId, currentPageSize, onPageSizeChange) {
            const container = document.getElementById(containerId);
            if (!container) return;
            if (total === 0) { container.innerHTML = ''; return; }

            const start = (current - 1) * pageSize + 1;
            const end = Math.min(current * pageSize, total);

            // 生成页码按钮（最多显示前后 2 页 + 首尾页 + 省略号）
            const pages = [];
            const addPage = p => pages.push(`<button class="page-btn ${p === current ? 'active' : ''}" data-page="${p}">${p}</button>`);
            if (totalPages <= 7) {
                for (let p = 1; p <= totalPages; p++) addPage(p);
            } else {
                addPage(1);
                if (current > 3) pages.push(`<span class="page-ellipsis">...</span>`);
                for (let p = Math.max(2, current - 1); p <= Math.min(totalPages - 1, current + 1); p++) addPage(p);
                if (current < totalPages - 2) pages.push(`<span class="page-ellipsis">...</span>`);
                addPage(totalPages);
            }

            const sizeOpts = [10, 20, 50, 100].map(s =>
                `<option value="${s}" ${s === currentPageSize ? 'selected' : ''}>${s}</option>`
            ).join('');

            container.innerHTML = `
                <div class="pagination">
                    <div class="pagination-info">${start}-${end} / ${total}</div>
                    <div class="pagination-right">
                        <span class="page-size-label">${t('per_page')}</span>
                        <select class="page-size-inline" id="${pageSizeSelectId}">${sizeOpts}</select>
                        <div class="pagination-controls">
                            <button class="page-btn" data-page="${current - 1}" ${current === 1 ? 'disabled' : ''}>‹</button>
                            ${pages.join('')}
                            <button class="page-btn" data-page="${current + 1}" ${current === totalPages ? 'disabled' : ''}>›</button>
                        </div>
                    </div>
                </div>
            `;
            // 绑定页码点击事件
            container.querySelectorAll('.page-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const p = parseInt(btn.dataset.page, 10);
                    if (!isNaN(p) && p >= 1 && p <= totalPages && p !== current) onPage(p);
                });
            });
            // 绑定每页条数变化事件
            const sizeSel = container.querySelector(`#${pageSizeSelectId}`);
            if (sizeSel) {
                sizeSel.addEventListener('change', e => onPageSizeChange(parseInt(e.target.value, 10)));
            }
        }

                /**
         * 更新历史视图的任务名筛选下拉框
         * 从当前 history 中提取所有任务名并去重排序，保留当前选中项。
         */
        function updateJobDropdown() {
            const select = document.getElementById('history-job-filter');
            const current = select.value;
            const names = [...new Set(data.history.map(h => h.func_name))].sort();

            select.innerHTML = `<option value="all">${t('all_jobs')}</option>` +
                names.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');

            if (names.includes(current) || current === 'all') {
                select.value = current;
            }
        }

        // Actions
        // ===== Add Task Modal =====
        // 当前任务源类型：'script-file' 或 'inline-code'
        let currentSource = 'script-file';
        // 编辑模式：非空时表示正在编辑该 job_id 对应的任务，null 表示新增模式
        let editingJobId = null;

                /**
         * 打开添加任务弹窗（新增模式）
         * 先重置表单清空旧数据，再显示遮罩层与弹窗。
         */
        function openAddTaskModal() {
            // 打开前先重置表单，清空之前填写的数据
            resetAddTaskForm();
            setAddTaskModalMode(false);
            document.getElementById('add-task-modal').classList.add('active');
        }

                /** 关闭添加任务弹窗（移除 active 类，并重置编辑状态） */
        function closeAddTaskModal() {
            document.getElementById('add-task-modal').classList.remove('active');
            editingJobId = null;
            setAddTaskModalMode(false);
        }

                /**
         * 切换添加/编辑任务弹窗的模式
         * @param {boolean} isEdit - true=编辑模式，false=新增模式
         */
        function setAddTaskModalMode(isEdit) {
            const title = document.getElementById('add-task-modal-title');
            const submitBtn = document.getElementById('submit-task-btn');
            const funcNameInput = document.getElementById('task-func-name');
            const tabScriptFile = document.getElementById('tab-script-file');
            const tabInlineCode = document.getElementById('tab-inline-code');
            if (isEdit) {
                title.textContent = t('edit_task_title');
                submitBtn.textContent = t('save');
                // 编辑模式禁止修改任务名称（作为任务唯一标识的一部分）
                funcNameInput.disabled = true;
                funcNameInput.classList.add('input-disabled');
                // 编辑模式下禁止切换任务源（由 editJob 根据原任务源锁定显示）
                tabScriptFile.disabled = true;
                tabInlineCode.disabled = true;
            } else {
                title.textContent = t('add_new_task');
                submitBtn.textContent = t('add_task');
                editingJobId = null;
                funcNameInput.disabled = false;
                funcNameInput.classList.remove('input-disabled');
                // 新增模式：恢复两个 tab 可见且可切换
                tabScriptFile.disabled = false;
                tabInlineCode.disabled = false;
                tabScriptFile.style.display = '';
                tabInlineCode.style.display = '';
            }
        }

                /**
         * 打开编辑任务弹窗（编辑模式）
         * 仅当任务处于暂停状态时可调用。拉取任务详情后回填表单。
         * @param {string} jobId - 要编辑的任务 ID
         */
        async function editJob(jobId) {
            try {
                const res = await fetch(`${API}/api/jobs/${jobId}`);
                const d = await res.json();
                if (!d.job) {
                    toast('error', t('t_job_not_found'));
                    return;
                }
                const job = d.job;
                // 二次校验：仅允许编辑已暂停的任务
                if (!job.paused) {
                    toast('error', t('t_job_not_paused'));
                    return;
                }

                // 先重置表单到初始状态，再回填
                resetAddTaskForm();
                editingJobId = jobId;
                setAddTaskModalMode(true);

                // 回填任务源（编辑模式下只显示当前源对应的 tab，隐藏另一个）
                if (job.script_code) {
                    switchSource('inline-code');
                    document.getElementById('task-script-code').value = job.script_code;
                    // 只显示 Inline Code tab，隐藏 Script File tab
                    document.getElementById('tab-script-file').style.display = 'none';
                    document.getElementById('tab-inline-code').style.display = '';
                } else if (job.script_path) {
                    switchSource('script-file');
                    document.getElementById('task-script-path').value = job.script_path;
                    // 只显示 Script File tab，隐藏 Inline Code tab
                    document.getElementById('tab-inline-code').style.display = 'none';
                    document.getElementById('tab-script-file').style.display = '';
                }
                // 回填脚本参数（列表 -> 空格分隔字符串）
                if (Array.isArray(job.script_args) && job.script_args.length) {
                    document.getElementById('task-script-args').value = job.script_args.join(' ');
                }
                // 回填任务名
                if (job.func_name) {
                    document.getElementById('task-func-name').value = job.func_name;
                }
                // 回填调度类型及对应字段
                if (job.schedule_type) {
                    document.getElementById('task-schedule-type').value = job.schedule_type;
                    onScheduleTypeChange();
                    if (job.schedule_type === 'interval' && job.interval != null) {
                        document.getElementById('task-interval').value = job.interval;
                    } else if (job.schedule_type === 'daily' && job.schedule_time) {
                        document.getElementById('task-schedule-time').value = job.schedule_time;
                    } else if (job.schedule_type === 'cron' && job.cron_expression) {
                        applyCronPreset(job.cron_expression);
                    } else if (job.schedule_type === 'once' && job.interval != null) {
                        document.getElementById('task-once-delay').value = job.interval;
                    }
                }
                // 回填其它可选字段
                if (job.max_retries != null) {
                    document.getElementById('task-max-retries').value = job.max_retries;
                }
                if (job.timeout != null) {
                    document.getElementById('task-timeout').value = job.timeout;
                }
                if (job.timezone) {
                    document.getElementById('task-timezone').value = job.timezone;
                }
                document.getElementById('task-catch-up').checked = !!job.catch_up;

                document.getElementById('add-task-modal').classList.add('active');
            } catch (e) {
                toast('error', t('t_request_failed', e.message));
            }
        }

                /**
         * 切换任务源类型（Script File / Inline Code，二者互斥）
         * @param {string} source - 'script-file' | 'inline-code'
         */
        function switchSource(source) {
            currentSource = source;
            // 切换标签激活状态
            document.getElementById('tab-script-file').classList.toggle('active', source === 'script-file');
            document.getElementById('tab-inline-code').classList.toggle('active', source === 'inline-code');
            // 切换面板显示
            document.getElementById('panel-script-file').classList.toggle('active', source === 'script-file');
            document.getElementById('panel-inline-code').classList.toggle('active', source === 'inline-code');
            // 内联代码模式下隐藏脚本参数（仅脚本文件模式支持命令行参数）
            document.getElementById('script-args-group').style.display = source === 'script-file' ? '' : 'none';
        }

                /**
         * 调度类型切换时显示/隐藏对应的字段区域
         * 支持: interval(间隔) / daily(每日) / cron(表达式) / once(一次性)
         */
        function onScheduleTypeChange() {
            const type = document.getElementById('task-schedule-type').value;
            ['interval', 'daily', 'cron', 'once'].forEach(st => {
                document.getElementById(`fields-${st}`).classList.toggle('active', st === type);
            });
        }

                /**
         * 提交添加/编辑任务表单
         * 新增模式: 构建请求体 → POST /api/scripts
         * 编辑模式: 构建请求体 → PUT /api/jobs/{job_id}
         * 成功则关闭弹窗并重置表单，失败弹 Toast。
         */
        async function submitAddTask() {
            const btn = document.getElementById('submit-task-btn');
            const isEdit = !!editingJobId;
            btn.disabled = true;
            btn.textContent = isEdit ? t('saving') : t('adding');

            try {
                const payload = buildTaskPayload();
                if (payload.error) {
                    toast('error', payload.error);
                    return;
                }

                let res, d;
                if (isEdit) {
                    // 编辑模式：PUT /api/jobs/{job_id}
                    res = await fetch(`${API}/api/jobs/${editingJobId}`, {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload.data)
                    });
                } else {
                    // 新增模式：POST /api/scripts
                    res = await fetch(`${API}/api/scripts`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload.data)
                    });
                }
                d = await res.json();

                if (d.success) {
                    toast('success', isEdit ? t('t_task_updated') : t('t_task_added', d.job_id));
                    closeAddTaskModal();
                    resetAddTaskForm();
                } else {
                    let msg = d.error || (isEdit ? t('t_update_failed') : t('t_add_failed'));
                    // 识别后端重名错误码 DUPLICATE_NAME:<name>，翻译为当前语言
                    if (msg.startsWith('DUPLICATE_NAME:')) {
                        msg = t('t_job_name_exists', msg.substring('DUPLICATE_NAME:'.length));
                    }
                    toast('error', msg);
                }
            } catch (e) {
                toast('error', t('t_request_failed', e.message));
            } finally {
                btn.disabled = false;
                btn.textContent = isEdit ? t('save') : t('add_task');
            }
        }

                /**
         * 从表单收集数据构建提交给 API 的请求体
         * 校验必填项，根据任务源(script_path/script_code 互斥)和调度类型组装 payload。
         * @returns {{data: Object} | {error: string}} 成功返回 {data}，校验失败返回 {error}
         */
        function buildTaskPayload() {
            // 根据任务源类型获取 script_path 或 script_code（互斥）
            const data = {};
            if (currentSource === 'script-file') {
                const scriptPath = document.getElementById('task-script-path').value.trim();
                if (!scriptPath) {
                    return { error: t('t_script_path_required') };
                }
                data.script_path = scriptPath;
            } else {
                const scriptCode = document.getElementById('task-script-code').value.trim();
                if (!scriptCode) {
                    return { error: t('t_code_required') };
                }
                data.script_code = scriptCode;
            }

            const scheduleType = document.getElementById('task-schedule-type').value;
            data.schedule_type = scheduleType;

            // Schedule-specific fields
            if (scheduleType === 'interval') {
                const interval = parseFloat(document.getElementById('task-interval').value);
                if (!interval || interval <= 0) {
                    return { error: t('t_interval_required') };
                }
                data.interval = interval;
            } else if (scheduleType === 'daily') {
                const time = document.getElementById('task-schedule-time').value;
                if (!time) {
                    return { error: t('t_daily_time_required') };
                }
                data.schedule_time = time;
            } else if (scheduleType === 'cron') {
                const expr = document.getElementById('task-cron-expression').value.trim();
                if (!expr) {
                    return { error: t('t_cron_required') };
                }
                data.cron_expression = expr;
            } else if (scheduleType === 'once') {
                const delay = parseFloat(document.getElementById('task-once-delay').value);
                if (!delay || delay <= 0) {
                    return { error: t('t_delay_required') };
                }
                data.interval = delay;
            }

            // Optional: task name（编辑模式禁止修改，不发送该字段）
            if (!editingJobId) {
                const funcName = document.getElementById('task-func-name').value.trim();
                if (funcName) data.func_name = funcName;
            }

            // Optional: script args (space-separated -> list, 仅脚本文件模式)
            if (currentSource === 'script-file') {
                const argsStr = document.getElementById('task-script-args').value.trim();
                if (argsStr) {
                    data.script_args = argsStr.split(/\s+/).filter(Boolean);
                }
            }

            // Optional: max retries
            const maxRetries = parseInt(document.getElementById('task-max-retries').value);
            if (!isNaN(maxRetries)) data.max_retries = maxRetries;

            // Optional: timeout
            const timeout = parseFloat(document.getElementById('task-timeout').value);
            if (!isNaN(timeout) && timeout > 0) data.timeout = timeout;

            // 时区（下拉选择，默认 Asia/Shanghai）
            const timezone = document.getElementById('task-timezone').value;
            if (timezone) data.timezone = timezone;

            // Catch up
            data.catch_up = document.getElementById('task-catch-up').checked;

            return { data };
        }

                /** 重置添加任务表单所有字段为初始值 */
        function resetAddTaskForm() {
            document.getElementById('add-task-form').reset();
            document.getElementById('task-max-retries').value = 0;
            document.getElementById('task-timeout').value = 300;
            document.getElementById('task-catch-up').checked = true;
            document.getElementById('task-schedule-time').value = '09:00';
            document.getElementById('task-timezone').value = 'Asia/Shanghai';
            switchSource('script-file');
            onScheduleTypeChange();
            // 重置 cron 配置面板为默认（每分钟）
            applyCronPreset('* * * * *');
        }

                /** 暂停指定任务: POST /api/jobs/{id}/pause */
        async function pauseJob(id) {
            if (!confirm(t('t_confirm_pause'))) return;
            const res = await fetch(`${API}/api/jobs/${id}/pause`, { method: 'POST' });
            const d = await res.json();
            toast(d.success ? 'success' : 'error', d.success ? t('t_job_paused') : d.error);
        }

                /** 恢复指定任务: POST /api/jobs/{id}/resume */
        async function resumeJob(id) {
            if (!confirm(t('t_confirm_resume'))) return;
            const res = await fetch(`${API}/api/jobs/${id}/resume`, { method: 'POST' });
            const d = await res.json();
            toast(d.success ? 'success' : 'error', d.success ? t('t_job_resumed') : d.error);
        }

                /** 取消指定任务(带确认): POST /api/jobs/{id}/cancel */
        async function cancelJob(id) {
            if (!confirm(t('t_confirm_cancel'))) return;
            const res = await fetch(`${API}/api/jobs/${id}/cancel`, { method: 'POST' });
            const d = await res.json();
            toast(d.success ? 'success' : 'error', d.success ? t('t_job_cancelled') : d.error);
        }

                /** 立即触发指定任务执行一次（带确认）: POST /api/jobs/{id}/run */
        async function runJobNow(id) {
            if (!confirm(t('t_confirm_run'))) return;
            const res = await fetch(`${API}/api/jobs/${id}/run`, { method: 'POST' });
            const d = await res.json();
            toast(d.success ? 'success' : 'error', d.success ? t('t_job_triggered') : d.error);
        }

                /**
         * 查看任务详情: GET /api/jobs/{id}
         * 拉取单个任务的完整信息并填充到详情模态框中显示。
         */
        async function viewJobDetail(id) {
            try {
                const res = await fetch(`${API}/api/jobs/${id}`);
                const d = await res.json();
                if (!d.job) {
                    toast('error', d.error ? t('t_job_not_found') : t('t_load_detail_failed'));
                    return;
                }
                const job = d.job;
                document.getElementById('detail-job-id').textContent = esc(job.job_id);
                document.getElementById('detail-func-name').textContent = esc(job.func_name);
                const statusEl = document.getElementById('detail-status');
                statusEl.textContent = t(job.status);
                statusEl.className = `job-status ${job.status}`;
                document.getElementById('detail-schedule').textContent = esc(job.schedule || '-');
                document.getElementById('detail-next-run').textContent = esc(job.next_run || '-');
                document.getElementById('detail-countdown').textContent = job.paused ? '—' : formatCountdown(job.next_run_in);
                document.getElementById('detail-run-count').textContent = job.run_count ?? 0;
                document.getElementById('detail-retry-count').textContent = job.retry_count ?? 0;
                document.getElementById('detail-max-retries').textContent = job.max_retries ?? 0;
                document.getElementById('detail-timeout').textContent = job.timeout != null ? job.timeout : t('d_none');
                document.getElementById('detail-last-run').textContent = esc(job.last_run || t('d_none'));
                document.getElementById('detail-paused').textContent = job.paused ? t('d_yes') : t('d_no');

                // 脚本路径：仅脚本文件模式任务显示
                const pathRow = document.getElementById('detail-script-path-row');
                const pathEl = document.getElementById('detail-script-path');
                if (job.script_path) {
                    pathEl.textContent = job.script_path;
                    pathRow.style.display = '';
                } else {
                    pathRow.style.display = 'none';
                }

                // 脚本参数：仅有参数时显示（列表 -> 空格分隔字符串）
                const argsRow = document.getElementById('detail-script-args-row');
                const argsEl = document.getElementById('detail-script-args');
                if (Array.isArray(job.script_args) && job.script_args.length) {
                    argsEl.textContent = job.script_args.join(' ');
                    argsRow.style.display = '';
                } else {
                    argsRow.style.display = 'none';
                }

                // 内联代码：仅动态代码模式任务显示
                const codeRow = document.getElementById('detail-script-code-row');
                const codeEl = document.getElementById('detail-script-code');
                if (job.script_code) {
                    codeEl.textContent = job.script_code;
                    codeRow.style.display = '';
                } else {
                    codeRow.style.display = 'none';
                }

                document.getElementById('job-detail-modal').classList.add('active');
            } catch (e) {
                console.error('viewJobDetail error:', e);
                toast('error', t('t_load_detail_failed'));
            }
        }

                /** 关闭任务详情模态框 */
        function closeJobDetailModal() {
            document.getElementById('job-detail-modal').classList.remove('active');
        }

        /**
         * 查看执行记录详情（历史/失败视图共用）
         * 根据类型从缓存取条目，填充并展示执行详情模态框。
         * @param {string} type - 'history' | 'failed'
         * @param {number} idx  - 条目在当前页缓存中的索引
         */
        function viewExecutionDetail(type, idx) {
            const items = type === 'history' ? historyItemsCache : failedItemsCache;
            const item = items[idx];
            if (!item) return;

            // 状态：仅历史视图有 status 字段，失败视图全部为 failed
            const statusRow = document.getElementById('exec-detail-status-row');
            const statusEl = document.getElementById('exec-detail-status');
            if (item.status) {
                statusEl.textContent = t(item.status);
                statusEl.className = `history-status ${item.status}`;
                statusRow.style.display = '';
            } else {
                statusEl.textContent = t('failed');
                statusEl.className = 'history-status failed';
                statusRow.style.display = '';
            }

            document.getElementById('exec-detail-job').textContent = item.func_name || '-';
            document.getElementById('exec-detail-time').textContent = item.timestamp_readable || formatTs(item.timestamp);
            document.getElementById('exec-detail-duration').textContent = item.execution_time ? item.execution_time.toFixed(2) + 's' : '-';
            document.getElementById('exec-detail-run-no').textContent = item.run_count != null ? item.run_count : '-';
            document.getElementById('exec-detail-retries').textContent = item.retry_count != null ? item.retry_count : 0;

            // 错误信息：有则展示，无则隐藏该行
            const errorRow = document.getElementById('exec-detail-error-row');
            const errorEl = document.getElementById('exec-detail-error');
            if (item.error) {
                errorEl.textContent = item.error;
                errorRow.style.display = '';
            } else {
                errorEl.textContent = '';
                errorRow.style.display = 'none';
            }

            document.getElementById('execution-detail-modal').classList.add('active');
        }

        /** 关闭执行详情模态框 */
        function closeExecutionDetailModal() {
            document.getElementById('execution-detail-modal').classList.remove('active');
        }

                /** 清空死信队列所有失败记录(带确认): DELETE /api/dead-letters */
        async function clearDeadLetters() {
            if (!confirm(t('t_confirm_clear_dlq'))) return;
            const res = await fetch(`${API}/api/dead-letters`, { method: 'DELETE' });
            const d = await res.json();
            toast(d.success ? 'success' : 'error', d.success ? t('t_cleared', d.cleared) : t('t_clear_failed'));
            if (d.success) {
                failedPage = 1;
                fetchFailed();
            }
        }

        // Toast
                /**
         * 显示临时 Toast 消息
         * @param {string} type - 'success' | 'error'
         * @param {string} msg  - 消息文本
         * 3 秒后自动淡出并移除。
         */
        function toast(type, msg) {
            const el = document.createElement('div');
            el.className = `toast ${type}`;
            el.innerHTML = `<div class="toast-icon">${type === 'success' ? '✓' : '✗'}</div><div class="toast-text">${esc(msg)}</div>`;
            // error 类型直接挂到 body，脱离 toast-container 的层叠上下文，
            // 确保能覆盖在模态框（z-index:1000）之上显示
            if (type === 'error') {
                document.body.appendChild(el);
            } else {
                document.getElementById('toast-container').appendChild(el);
            }
            setTimeout(() => {
                el.style.opacity = '0';
                setTimeout(() => el.remove(), 200);
            }, 3000);
        }

        // Utils
                /**
         * 格式化倒计时秒数为人类可读字符串
         * @param {number} s - 剩余秒数
         * @returns {string} 如 'Now' / '45s' / '5m 30s' / '2h 15m' / '3d'
         */
        function formatCountdown(s) {
            if (s <= 0) return t('now');
            if (s < 60) return `${Math.round(s)}s`;
            if (s < 3600) return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
            if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
            return `${Math.floor(s / 86400)}d`;
        }

        /**
         * 格式化运行时长，按当前语言输出
         * 中文: '1天3小时45分12秒'
         * 英文: '1d 3h 45m 12s'
         */
        function formatUptime(s) {
            if (s == null || isNaN(s) || s < 0) return '--';
            s = Math.floor(s);
            const days = Math.floor(s / 86400);
            const hours = Math.floor((s % 86400) / 3600);
            const minutes = Math.floor((s % 3600) / 60);
            const secs = s % 60;
            const lang = getCurrentLang();
            // 有更高单位时，分钟和秒始终显示（含 0），避免 '1小时30秒' 这种缺分钟的情况
            const hasHigher = days > 0 || hours > 0;
            if (lang === 'zh') {
                const parts = [];
                if (days > 0) parts.push(`${days}天`);
                if (hours > 0) parts.push(`${hours}小时`);
                if (minutes > 0 || hasHigher) parts.push(`${minutes}分`);
                if (secs > 0 || !parts.length) parts.push(`${secs}秒`);
                return parts.join('');
            } else {
                const parts = [];
                if (days > 0) parts.push(`${days}d`);
                if (hours > 0) parts.push(`${hours}h`);
                if (minutes > 0 || hasHigher) parts.push(`${minutes}m`);
                if (secs > 0 || !parts.length) parts.push(`${secs}s`);
                return parts.join(' ');
            }
        }

                /**
         * 从 'YYYY-MM-DD HH:MM:SS' 格式时间字符串中提取时间部分
         * @returns {string} 'HH:MM:SS'，解析失败返回原字符串
         */
        function formatTime(str) {
            if (!str) return '';
            const parts = str.split(' ');
            return parts[1] || str;
        }

                /**
         * 将 Unix 时间戳(秒)格式化为本地时间字符串
         * @param {number} ts - Unix 时间戳
         */
        function formatTs(ts) {
            return ts ? new Date(ts * 1000).toLocaleString() : '';
        }

                /**
         * HTML 转义函数，防止 XSS
         * 通过创建临时 div 的 textContent→innerHTML 机制转义 < > & " 等。
         * @returns {string} 转义后的安全字符串
         */
        function esc(s) {
            if (!s) return '';
            const d = document.createElement('div');
            d.textContent = s;
            return d.innerHTML;
        }

        window.addEventListener('beforeunload', () => eventSource?.close());
    