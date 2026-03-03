(function () {
  let csrf = document.querySelector('meta[name="csrf-token"]')?.getAttribute('content') || '';
  const toastRoot = document.getElementById('toast-root');

  const toast = (msg, kind = 'ok') => {
    if (!toastRoot) return;
    const el = document.createElement('div');
    el.className = `toast ${kind}`;
    el.textContent = msg;
    toastRoot.appendChild(el);
    setTimeout(() => el.classList.add('show'), 10);
    setTimeout(() => {
      el.classList.remove('show');
      setTimeout(() => el.remove(), 250);
    }, 2500);
  };

  const themeToggle = document.getElementById('theme-toggle');
  const savedTheme = localStorage.getItem('db_theme') || 'dark';
  document.documentElement.setAttribute('data-theme', savedTheme);
  if (themeToggle) {
    themeToggle.addEventListener('click', () => {
      const cur = document.documentElement.getAttribute('data-theme') || 'dark';
      const next = cur === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('db_theme', next);
    });
  }

  const sidebar = document.getElementById('sidebar');
  const toggle = document.getElementById('sidebar-toggle');
  if (sidebar && toggle) {
    toggle.addEventListener('click', () => sidebar.classList.toggle('open'));
  }

  const navItems = document.querySelectorAll('.nav-item[data-nav]');
  const sections = document.querySelectorAll('.section[data-section]');
  const storageKey = 'db_nav_active';
  let analyticsInit = false;
  const maybeInitAnalytics = (key) => {
    if (key === 'analytics' && !analyticsInit) {
      setupAnalytics();
      analyticsInit = true;
    }
  };
  const activate = (key) => {
    navItems.forEach((n) => n.classList.toggle('active', n.dataset.nav === key));
    sections.forEach((s) => s.classList.toggle('active', s.dataset.section === key));
    localStorage.setItem(storageKey, key);
    maybeInitAnalytics(key);
    window.scrollTo(0, 0);
  };

  const remembered = localStorage.getItem(storageKey);
  if (remembered && document.querySelector(`.section[data-section="${remembered}"]`)) {
    activate(remembered);
  }

  navItems.forEach((item) => {
    item.addEventListener('click', () => {
      activate(item.dataset.nav);
      if (sidebar && window.innerWidth <= 980) sidebar.classList.remove('open');
    });
  });

  const welcomeForm = document.getElementById('welcome-form');
  if (welcomeForm) {
    const title = welcomeForm.querySelector('input[name="title"]');
    const desc = welcomeForm.querySelector('textarea[name="description"]');
    const footer = welcomeForm.querySelector('input[name="footer"]');
    const color = welcomeForm.querySelector('input[name="color"]');
    const pvTitle = document.getElementById('pv-title');
    const pvDesc = document.getElementById('pv-desc');
    const pvFooter = document.getElementById('pv-footer');
    const preview = document.getElementById('welcome-preview');
    const sync = () => {
      if (pvTitle) pvTitle.textContent = title?.value || '';
      if (pvDesc) pvDesc.textContent = desc?.value || '';
      if (pvFooter) pvFooter.textContent = footer?.value || '';
      if (preview && color?.value) preview.style.borderColor = color.value;
    };
    [title, desc, footer, color].forEach((el) => el && el.addEventListener('input', sync));
  }

  document.querySelectorAll('[data-module-form]').forEach((form) => {
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const module = form.getAttribute('data-module');
      const guild = form.getAttribute('data-guild');
      const payload = {};
      form.querySelectorAll('input, textarea, select').forEach((el) => {
        if (!el.name) return;
        if (el.type === 'checkbox') {
          payload[el.name] = el.checked;
          return;
        }
        if (el.tagName === 'SELECT' && el.multiple) {
          payload[el.name] = Array.from(el.selectedOptions).map((opt) => opt.value);
          return;
        }
        payload[el.name] = el.value;
      });

      if (module === 'automod' && typeof payload.banned_words === 'string') {
        payload.banned_words = payload.banned_words
          .split(/[\n,]+/)
          .map((x) => x.trim())
          .filter(Boolean);
      }
      if (module === 'automod' && typeof payload.allowed_domains === 'string') {
        payload.allowed_domains = payload.allowed_domains
          .split(/[\n,]+/)
          .map((x) => x.trim().toLowerCase())
          .filter(Boolean);
      }

      const submit = form.querySelector('button[type="submit"]');
      const oldText = submit ? submit.textContent : '';
      if (submit) {
        submit.disabled = true;
        submit.textContent = 'Saving...';
      }

      const saveRequest = () => fetch(`/dashboard/save/${guild}/${module}`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRF-Token': csrf,
          },
          body: JSON.stringify(payload),
        });

      try {
        let res = await saveRequest();
        let out = await res.json();
        if (res.status === 403 && out?.error === 'csrf_failed' && out?.csrf_token) {
          csrf = String(out.csrf_token);
          res = await saveRequest();
          out = await res.json();
        }
        if (!res.ok || !out.ok) throw new Error(out.error || 'Save failed');
        if (submit) submit.textContent = 'Saved';
        toast('Saved successfully', 'ok');
      } catch (err) {
        if (submit) submit.textContent = 'Error';
        toast(String(err), 'err');
      } finally {
        setTimeout(() => {
          if (submit) {
            submit.disabled = false;
            submit.textContent = oldText || 'Save';
          }
        }, 900);
      }
    });
  });

  const settingsForm = document.querySelector('form[data-module-form][data-module="settings"]');
  const collectFormPayload = (form) => {
    const payload = {};
    form.querySelectorAll('input, textarea, select').forEach((el) => {
      if (!el.name) return;
      if (el.type === 'checkbox') {
        payload[el.name] = el.checked;
        return;
      }
      if (el.tagName === 'SELECT' && el.multiple) {
        payload[el.name] = Array.from(el.selectedOptions).map((opt) => opt.value);
        return;
      }
      payload[el.name] = el.value;
    });
    return payload;
  };

  const verifyApplyBtn = document.getElementById('verify-apply-lock');
  if (verifyApplyBtn && settingsForm) {
    verifyApplyBtn.addEventListener('click', async () => {
      const guild = verifyApplyBtn.getAttribute('data-guild');
      const payload = collectFormPayload(settingsForm);
      payload.guild_id = guild;
      payload.lock_enabled = true;
      const sendApply = () => fetch('/dashboard/api/verify/apply-lock', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
        body: JSON.stringify(payload),
      });
      try {
        let res = await sendApply();
        let out;
        try {
          out = await res.json();
        } catch (_) {
          const txt = await res.text();
          throw new Error(txt || `HTTP ${res.status}`);
        }
        if (res.status === 403 && out?.error === 'csrf_failed' && out?.csrf_token) {
          csrf = String(out.csrf_token);
          res = await sendApply();
          try {
            out = await res.json();
          } catch (_) {
            const txt = await res.text();
            throw new Error(txt || `HTTP ${res.status}`);
          }
        }
        if (!res.ok || !out.ok) throw new Error(out.error || 'apply_failed');
        const failed = Array.isArray(out.failed) ? out.failed.length : 0;
        toast(`Lock applied. Updated: ${out.changed ?? 0}, failed: ${failed}`, failed ? 'err' : 'ok');
      } catch (err) {
        toast(String(err), 'err');
      }
    });
  }

  const verifyRestoreBtn = document.getElementById('verify-restore-lock');
  if (verifyRestoreBtn && settingsForm) {
    verifyRestoreBtn.addEventListener('click', async () => {
      const guild = verifyRestoreBtn.getAttribute('data-guild');
      const sendRestore = () => fetch('/dashboard/api/verify/restore-lock', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
        body: JSON.stringify({ guild_id: guild }),
      });
      try {
        let res = await sendRestore();
        let out;
        try {
          out = await res.json();
        } catch (_) {
          const txt = await res.text();
          throw new Error(txt || `HTTP ${res.status}`);
        }
        if (res.status === 403 && out?.error === 'csrf_failed' && out?.csrf_token) {
          csrf = String(out.csrf_token);
          res = await sendRestore();
          try {
            out = await res.json();
          } catch (_) {
            const txt = await res.text();
            throw new Error(txt || `HTTP ${res.status}`);
          }
        }
        if (!res.ok || !out.ok) throw new Error(out.error || 'restore_failed');
        const failed = Array.isArray(out.failed) ? out.failed.length : 0;
        toast(`Permissions restored: ${out.restored ?? 0}, failed: ${failed}`, failed ? 'err' : 'ok');
      } catch (err) {
        toast(String(err), 'err');
      }
    });
  }

  const stats = document.querySelector('[data-stats]');
  let guild = null;
  if (stats) guild = stats.getAttribute('data-guild');
  if (!guild) {
    const supportGuildNode = document.getElementById('support-center') || document.getElementById('support-create-form');
    if (supportGuildNode) guild = supportGuildNode.getAttribute('data-guild');
  }
  const diagRoot = document.getElementById('support-diagnostics');
  const diagSummary = document.getElementById('diag-summary');
  const diagDetails = document.getElementById('diag-details');
  const diagRefresh = document.getElementById('diag-refresh');

  const set = (k, v) => {
    const node = document.querySelector(`[data-stat="${k}"]`);
    if (!node) return;
    node.textContent = k === 'latency_ms' ? `${v} ms` : String(v);
  };

  const formatTs = (ts) => {
    const n = Number(ts || 0);
    if (!n) return '-';
    try {
      return new Date(n * 1000).toLocaleString();
    } catch (_) {
      return '-';
    }
  };

  const postJsonWithCsrf = async (url, payload) => {
    const send = () => fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
      body: JSON.stringify(payload || {}),
    });
    let res = await send();
    let out = {};
    try {
      out = await res.json();
    } catch (_) {}
    if (res.status === 403 && out?.error === 'csrf_failed' && out?.csrf_token) {
      csrf = String(out.csrf_token);
      res = await send();
      try {
        out = await res.json();
      } catch (_) {
        out = {};
      }
    }
    return { res, out };
  };

  const supportCenter = document.getElementById('support-center');
  const supportCreateForm = document.getElementById('support-create-form');
  const supportCasesList = document.getElementById('support-cases-list');
  const supportMessages = document.getElementById('support-messages');
  const supportThreadTitle = document.getElementById('support-thread-title');
  const supportThreadMeta = document.getElementById('support-thread-meta');
  const supportReplyForm = document.getElementById('support-reply-form');
  const supportAssignMe = document.getElementById('support-assign-me');
  const supportUnassign = document.getElementById('support-unassign');
  const supportMarkResolved = document.getElementById('support-mark-resolved');
  const supportMarkOpen = document.getElementById('support-mark-open');
  const supportInboxRoot = document.getElementById('support-inbox-root');
  const supportInboxList = document.getElementById('support-inbox-list');
  const supportFilterStatus = document.getElementById('support-filter-status');
  const supportFilterPriority = document.getElementById('support-filter-priority');
  const supportFilterQ = document.getElementById('support-filter-q');
  const supportVisibilityInternal = document.getElementById('support-visibility-internal');
  const supportQuickNeedSteps = document.getElementById('support-quick-need-steps');
  const supportQuickFixed = document.getElementById('support-quick-fixed');
  const supportPrivileged = supportCenter?.getAttribute('data-support-privileged') === '1';
  let supportCases = [];
  let supportSelectedCaseId = null;
  if (supportVisibilityInternal && !supportPrivileged) {
    supportVisibilityInternal.closest('label')?.classList.add('support-hidden');
  }
  if (supportAssignMe && !supportPrivileged) supportAssignMe.classList.add('support-hidden');
  if (supportUnassign && !supportPrivileged) supportUnassign.classList.add('support-hidden');

  const renderSupportCases = () => {
    if (!supportCasesList) return;
    if (!supportCases.length) {
      supportCasesList.innerHTML = '<p class="muted">No support cases yet.</p>';
      return;
    }
    supportCasesList.innerHTML = '';
    supportCases.forEach((c) => {
      const el = document.createElement('div');
      el.className = `support-case-item${Number(c.id) === Number(supportSelectedCaseId) ? ' active' : ''}`;
      const badges = [
        `<span class="support-badge">${c.status}</span>`,
        `<span class="support-badge">${c.priority}</span>`,
      ];
      if (c.sla_breached) badges.push('<span class="support-badge sla">SLA breached</span>');
      el.innerHTML = `
        <div><strong>#${c.id}</strong> ${c.subject || ''}</div>
        <div class="support-case-meta">${badges.join(' ')} ${c.guild_name || c.guild_id}</div>
        <div class="support-case-meta">Updated: ${formatTs(c.updated_at)} | Waiting on: ${c.waiting_on || '-'}</div>
      `;
      el.addEventListener('click', () => {
        supportSelectedCaseId = Number(c.id);
        renderSupportCases();
        loadSupportMessages();
      });
      supportCasesList.appendChild(el);
    });
  };

  const loadSupportCases = async () => {
    if (!guild || !supportCenter) return;
    try {
      const status = encodeURIComponent(String(supportFilterStatus?.value || ''));
      const priority = encodeURIComponent(String(supportFilterPriority?.value || ''));
      const q = encodeURIComponent(String(supportFilterQ?.value || '').trim());
      const res = await fetch(`/dashboard/api/support/cases?guild_id=${guild}&status=${status}&priority=${priority}&q=${q}`);
      if (!res.ok) return;
      const out = await res.json();
      supportCases = Array.isArray(out.cases) ? out.cases : [];
      if (!supportSelectedCaseId && supportCases.length) supportSelectedCaseId = Number(supportCases[0].id);
      if (supportSelectedCaseId && !supportCases.find((x) => Number(x.id) === Number(supportSelectedCaseId))) {
        supportSelectedCaseId = supportCases.length ? Number(supportCases[0].id) : null;
      }
      renderSupportCases();
      if (supportSelectedCaseId) loadSupportMessages();
    } catch (_) {}
  };

  const loadSupportMessages = async () => {
    if (!supportSelectedCaseId || !supportMessages) return;
    try {
      const res = await fetch(`/dashboard/api/support/cases/${supportSelectedCaseId}/messages`);
      if (!res.ok) return;
      const out = await res.json();
      const msgs = Array.isArray(out.messages) ? out.messages : [];
      const c = out.case || {};
      if (supportThreadTitle) supportThreadTitle.textContent = `Case #${c.id || supportSelectedCaseId}: ${c.subject || ''}`;
      if (supportThreadMeta) {
        const fr = Number(c.first_response_at || 0);
        const rv = Number(c.resolved_at || 0);
        supportThreadMeta.textContent = `Status: ${c.status || '-'} | Priority: ${c.priority || '-'} | Assigned: ${c.assigned_to_name || 'Unassigned'} | First response: ${fr ? formatTs(fr) : '-'} | Resolved: ${rv ? formatTs(rv) : '-'}`;
      }
      supportMessages.innerHTML = msgs.map((m) => `
        <div class="support-msg">
          <div class="support-msg-head">${m.author_name} (${m.author_role}${m.visibility === 'internal' ? '/internal' : ''}) • ${formatTs(m.created_at)}</div>
          <div>${String(m.body || '').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</div>
        </div>
      `).join('') || '<p class="muted">No messages.</p>';
      supportMessages.scrollTop = supportMessages.scrollHeight;
    } catch (_) {}
  };

  const loadSupportInbox = async () => {
    if (!supportInboxRoot || !supportInboxList) return;
    try {
      const status = encodeURIComponent(String(supportFilterStatus?.value || ''));
      const priority = encodeURIComponent(String(supportFilterPriority?.value || ''));
      const q = encodeURIComponent(String(supportFilterQ?.value || '').trim());
      const res = await fetch(`/dashboard/api/support/cases?status=${status}&priority=${priority}&q=${q}`);
      if (!res.ok) return;
      const out = await res.json();
      const cases = Array.isArray(out.cases) ? out.cases : [];
      if (!cases.length) {
        supportInboxList.innerHTML = '<p class="muted">No cases in queue.</p>';
        return;
      }
      supportInboxList.innerHTML = cases.map((c) => `
        <a class="server-item" href="/dashboard/guild/${c.guild_id}">
          <span>#${c.id} ${c.subject} <small class="support-case-meta">(${c.status}/${c.priority}) ${c.sla_breached ? '[SLA]' : ''}</small></span>
          <small>${c.guild_name || c.guild_id}</small>
        </a>
      `).join('');
    } catch (_) {}
  };

  const pullStats = async () => {
    if (!guild) return;
    try {
      const res = await fetch(`/dashboard/api/stats/${guild}`);
      if (!res.ok) return;
      const s = await res.json();
      set('member_count', s.member_count || 0);
      set('online_count', s.online_count || 0);
      set('boost_level', s.boost_level || 0);
      set('latency_ms', s.latency_ms || 0);
    } catch (_) {}
  };

  const pullDiagnostics = async () => {
    if (!guild || !diagRoot || !diagSummary || !diagDetails) return;
    try {
      diagSummary.textContent = 'Refreshing diagnostics...';
      const res = await fetch(`/dashboard/api/diagnostics/${guild}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      const issues = Array.isArray(d.issues) ? d.issues : [];
      diagSummary.textContent = issues.length ? `Issues found: ${issues.join(', ')}` : 'No critical issues found.';
      diagDetails.textContent = JSON.stringify(d, null, 2);
    } catch (err) {
      diagSummary.textContent = 'Diagnostics failed.';
      diagDetails.textContent = String(err);
    }
  };

  const setupAnalytics = () => {
    if (!guild || typeof Chart === 'undefined') return;
    const canvas = document.getElementById('analytics-chart');
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    const chart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: ['join', 'leave', 'ban', 'message', 'module_save:welcome', 'module_save:automod'],
        datasets: [{
          label: 'Events',
          data: [0, 0, 0, 0, 0, 0],
          backgroundColor: '#4b8dff',
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        transitions: {
          active: { animation: { duration: 0 } },
          resize: { animation: { duration: 0 } },
          show: { animation: { duration: 0 } },
          hide: { animation: { duration: 0 } },
        },
        scales: {
          x: {
            ticks: { maxRotation: 0, minRotation: 0, autoSkip: true, maxTicksLimit: 8 },
          },
          y: { beginAtZero: true },
        },
      },
    });

    const loadRange = async (range) => {
      try {
        const res = await fetch(`/dashboard/api/analytics/${guild}?range=${range}`);
        if (!res.ok) return;
        const payload = await res.json();
        const m = payload.metrics || {};
        const labels = Object.keys(m);
        const vals = labels.map((k) => m[k]);
        chart.data.labels = labels.length ? labels : ['no_data'];
        chart.data.datasets[0].data = labels.length ? vals : [0];
        chart.update('none');
      } catch (_) {}
    };

    document.getElementById('analytics-daily')?.addEventListener('click', () => loadRange('daily'));
    document.getElementById('analytics-weekly')?.addEventListener('click', () => loadRange('weekly'));
    loadRange('daily');
  };

  if (guild) {
    if (supportCenter) {
      loadSupportCases();
      setInterval(loadSupportCases, 12000);
    }
    pullStats();
    pullDiagnostics();
    setInterval(pullStats, 12000);
    setInterval(pullDiagnostics, 30000);
    if (diagRefresh) {
      diagRefresh.addEventListener('click', pullDiagnostics);
    }

    const connectRealtime = async () => {
      try {
        const tokenRes = await fetch(`/dashboard/api/ws-token/${guild}`);
        if (!tokenRes.ok) throw new Error('ws_token_failed');
        const wsData = await tokenRes.json();
        const ws = new WebSocket(wsData.ws_url);

        ws.onmessage = (event) => {
          try {
            const payload = JSON.parse(event.data || '{}');
            if (payload.type === 'module_saved') {
              toast(`Updated: ${payload.module}`, 'ok');
            }
            if (payload.type === 'automod_trigger') {
              toast('Automod trigger detected', 'err');
            }
            if (payload.type === 'stats_update') {
              set('member_count', payload.member_count || 0);
              set('online_count', payload.online_count || 0);
              set('boost_level', payload.boost_level || 0);
              set('latency_ms', payload.latency_ms || 0);
            }
            if (payload.type === 'support_case_update' && supportCenter) {
              loadSupportCases();
            }
          } catch (_) {}
        };

        ws.onerror = () => {
          ws.close();
        };

        ws.onclose = () => {
          setTimeout(connectRealtime, 2500);
        };
      } catch (_) {
        setTimeout(connectRealtime, 3000);
      }
    };

    connectRealtime();
  }

  if (supportCreateForm && guild) {
    supportCreateForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(supportCreateForm);
      const payload = {
        guild_id: guild,
        subject: String(fd.get('subject') || ''),
        message: String(fd.get('message') || ''),
        priority: String(fd.get('priority') || 'normal'),
      };
      const { res, out } = await postJsonWithCsrf('/dashboard/api/support/cases', payload);
      if (!res.ok || !out.ok) {
        toast(out.error || 'Failed to create case', 'err');
        return;
      }
      supportCreateForm.reset();
      toast(`Case #${out.case_id} created`, 'ok');
      loadSupportCases();
    });
  }

  if (supportFilterStatus && supportCenter) {
    supportFilterStatus.addEventListener('change', () => loadSupportCases());
  }
  if (supportFilterPriority && supportCenter) {
    supportFilterPriority.addEventListener('change', () => loadSupportCases());
  }
  if (supportFilterQ && supportCenter) {
    supportFilterQ.addEventListener('input', () => loadSupportCases());
  }
  if (supportFilterStatus && supportInboxRoot) {
    supportFilterStatus.addEventListener('change', () => loadSupportInbox());
  }
  if (supportFilterPriority && supportInboxRoot) {
    supportFilterPriority.addEventListener('change', () => loadSupportInbox());
  }
  if (supportFilterQ && supportInboxRoot) {
    supportFilterQ.addEventListener('input', () => loadSupportInbox());
  }

  if (supportReplyForm) {
    supportReplyForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      if (!supportSelectedCaseId) return toast('Select a case first', 'err');
      const fd = new FormData(supportReplyForm);
      const message = String(fd.get('message') || '');
      const visibility = supportVisibilityInternal?.checked ? 'internal' : 'public';
      const { res, out } = await postJsonWithCsrf(`/dashboard/api/support/cases/${supportSelectedCaseId}/messages`, { message, visibility });
      if (!res.ok || !out.ok) {
        toast(out.error || 'Failed to send reply', 'err');
        return;
      }
      supportReplyForm.reset();
      if (supportVisibilityInternal) supportVisibilityInternal.checked = false;
      loadSupportCases();
      loadSupportMessages();
    });
  }

  if (supportQuickNeedSteps && supportReplyForm) {
    supportQuickNeedSteps.addEventListener('click', () => {
      const area = supportReplyForm.querySelector('textarea[name="message"]');
      if (!area) return;
      area.value = 'Thanks for reporting this. Please share exact steps to reproduce, expected result, and what happened instead.';
      area.focus();
    });
  }
  if (supportQuickFixed && supportReplyForm) {
    supportQuickFixed.addEventListener('click', () => {
      const area = supportReplyForm.querySelector('textarea[name="message"]');
      if (!area) return;
      area.value = 'We have applied a fix. Please test again now and confirm if the issue is resolved for you.';
      area.focus();
    });
  }

  if (supportAssignMe) {
    supportAssignMe.addEventListener('click', async () => {
      if (!supportSelectedCaseId) return toast('Select a case first', 'err');
      const { res, out } = await postJsonWithCsrf(`/dashboard/api/support/cases/${supportSelectedCaseId}/assign`, { action: 'assign_me' });
      if (!res.ok || !out.ok) return toast(out.error || 'Assign failed', 'err');
      loadSupportCases();
      loadSupportMessages();
    });
  }
  if (supportUnassign) {
    supportUnassign.addEventListener('click', async () => {
      if (!supportSelectedCaseId) return toast('Select a case first', 'err');
      const { res, out } = await postJsonWithCsrf(`/dashboard/api/support/cases/${supportSelectedCaseId}/assign`, { action: 'unassign' });
      if (!res.ok || !out.ok) return toast(out.error || 'Unassign failed', 'err');
      loadSupportCases();
      loadSupportMessages();
    });
  }
  if (supportMarkResolved) {
    supportMarkResolved.addEventListener('click', async () => {
      if (!supportSelectedCaseId) return toast('Select a case first', 'err');
      const { res, out } = await postJsonWithCsrf(`/dashboard/api/support/cases/${supportSelectedCaseId}/status`, { status: 'resolved' });
      if (!res.ok || !out.ok) return toast(out.error || 'Status update failed', 'err');
      loadSupportCases();
      loadSupportMessages();
    });
  }
  if (supportMarkOpen) {
    supportMarkOpen.addEventListener('click', async () => {
      if (!supportSelectedCaseId) return toast('Select a case first', 'err');
      const { res, out } = await postJsonWithCsrf(`/dashboard/api/support/cases/${supportSelectedCaseId}/status`, { status: 'open' });
      if (!res.ok || !out.ok) return toast(out.error || 'Status update failed', 'err');
      loadSupportCases();
      loadSupportMessages();
    });
  }

  if (supportInboxRoot) {
    loadSupportInbox();
    setInterval(loadSupportInbox, 10000);
    const connectSupportRealtime = async () => {
      try {
        const tokenRes = await fetch('/dashboard/api/ws-token/support:global');
        if (!tokenRes.ok) throw new Error('ws_token_failed');
        const wsData = await tokenRes.json();
        const ws = new WebSocket(wsData.ws_url);
        ws.onmessage = () => loadSupportInbox();
        ws.onerror = () => ws.close();
        ws.onclose = () => setTimeout(connectSupportRealtime, 2500);
      } catch (_) {
        setTimeout(connectSupportRealtime, 3000);
      }
    };
    connectSupportRealtime();
  }
})();
