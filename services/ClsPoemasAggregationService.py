import pandas as pd
import numpy as np

class ClsPoemasAggregationService:
    @staticmethod
    def aggregate_list_10ms_to_1s(data: list) -> list:
        """
        Agrega registros do POEMAS de 10 ms para 1 s por mediana.
        Entrada: lista de dicionários no formato do MongoDB.
        Saída: lista de dicionários no mesmo esquema, com um registro por segundo.
        """

        debug = False

        def dbg(msg, df_debug: pd.DataFrame | None = None):
            if debug:
                print(msg)
                if df_debug is not None:
                    print(df_debug.head(5))
                    print("----------------------------------------------------")

        # 1. validação da entrada
        # razão: evitar exceções e permitir rastreamento objetivo quando não houver dados
        if not data or len(data) == 0:
            dbg("[agg] lista de entrada vazia ou None. nada a agregar.")
            return []

        # 2. conversão para DataFrame para realizar operações vetorizadas
        # razão: pandas acelera agrupamentos, medianas e rederivações temporais
        df = pd.DataFrame(data)
        total_in = len(df)
        dbg(f"[agg] entrada recebida: n_registros={total_in}")

        # 3. normalização de tempo para datetime64[ns, UTC]
        # razão: garantir operações temporais consistentes, especialmente floor e groupby por segundo
        if "UTC_TIME" not in df.columns:
            dbg("[agg] campo UTC_TIME ausente. abortando.")
            return []

        if not np.issubdtype(df["UTC_TIME"].dtype, np.datetime64):
            df["UTC_TIME"] = pd.to_datetime(df["UTC_TIME"], utc=True, errors="coerce")
        else:
            if df["UTC_TIME"].dt.tz is None:
                df["UTC_TIME"] = df["UTC_TIME"].dt.tz_localize("UTC")
            else:
                df["UTC_TIME"] = df["UTC_TIME"].dt.tz_convert("UTC")

        # 4. ordenação temporal
        # razão: garantir determinismo e facilitar diagnósticos de faixa temporal
        df = df.sort_values("UTC_TIME").reset_index(drop=True)
        t_min = df["UTC_TIME"].iloc[0]
        t_max = df["UTC_TIME"].iloc[-1]
        dur_s = int((t_max - t_min).total_seconds())
        dbg(f"[agg] intervalo temporal de entrada: {t_min} -> {t_max}  duracao_aprox_s={dur_s}")

        # 5. estatística bruta de amostras por segundo antes de qualquer agregação
        # razão: checar cobertura e detectar jitter ou duplicatas
        df["SEC_START"] = df["UTC_TIME"].dt.floor("1s")
        cov = df.groupby("SEC_START", as_index=False).size().rename(columns={"size": "N_SAMPLES"})
        n_secs_in = len(cov)
        mean_sps = cov["N_SAMPLES"].mean()
        dbg(
            f"[agg] segundos distintos na entrada={n_secs_in}  "
            f"amostras_medianas_por_segundo={cov['N_SAMPLES'].median()}  "
            f"amostras_media_por_segundo={mean_sps:.2f}  "
            f"min={cov['N_SAMPLES'].min()}  "
            f"p95={cov['N_SAMPLES'].quantile(0.95)}  "
            f"max={cov['N_SAMPLES'].max()}"
        )
        dbg("[agg] segundos com menor cobertura:", cov.nsmallest(5, "N_SAMPLES"))
        dbg("[agg] segundos com maior cobertura:", cov.nlargest(5, "N_SAMPLES"))

        # 5.a coerência de intervalo temporal versus segundos observados
        # razão: medir lacunas de segundos e expectativa de cobertura
        t0_sec = t_min.floor("1s")
        t1_sec = t_max.floor("1s")
        expected_range_secs = int((t1_sec - t0_sec).total_seconds()) + 1
        missing_secs_est = expected_range_secs - n_secs_in
        dbg(
            f"[agg] coerencia_temporal: primeiro_segundo={t0_sec} ultimo_segundo={t1_sec} "
            f"segundos_no_intervalo_esperado={expected_range_secs} segundos_observados={n_secs_in} "
            f"estimativa_de_segundos_faltantes={missing_secs_est}"
        )

        # 6. checagem de gaps de segundos sem NumPy sobre objetos
        # razão: evitar erro ufunc 'divide' com dtype('O') ao operar datetimes com timezone
        sec_sorted = cov["SEC_START"].sort_values()
        if len(sec_sorted) > 1:
            gaps_s = sec_sorted.diff().dt.total_seconds().fillna(0)
            n_gaps = int((gaps_s > 1).sum())
            dbg(f"[agg] gaps_temporais_segundos_maiores_que_1s={n_gaps}")
            if n_gaps > 0:
                idx = gaps_s[gaps_s > 1].head(5).index
                exemplos = [(sec_sorted.loc[i - 1], sec_sorted.loc[i], int(gaps_s.loc[i])) for i in idx]
                dbg(f"[agg] exemplos de gaps: {exemplos}")

        # 7. seleção de campos numéricos a agregar por mediana
        # razão: estes campos carregam grandezas físicas contínuas
        float_fields_all = ["TBMAX", "TBMIN", "ELE", "AZI", "TBL45", "TBR45", "TBL90", "TBR90"]
        float_fields = [f for f in float_fields_all if f in df.columns]

        # 8. conversão para float para cálculo da mediana
        # razão: no banco podem vir como string; a mediana exige numérico
        for f in float_fields:
            df[f] = pd.to_numeric(df[f], errors="coerce")
        nan_conv = {f: int(df[f].isna().sum()) for f in float_fields}
        dbg(f"[agg] NaN apos conversao numerica: {nan_conv}")

        # 9. diagnóstico de segundos com 200 amostras
        # razão: distinguir duplicatas por tick de 10 ms de jitter real
        df["TICK_10MS"] = df["UTC_TIME"].dt.floor("10ms")
        uniq_per_sec = df.groupby("SEC_START")["TICK_10MS"].nunique().rename("N_TICKS_10MS")
        cov = cov.merge(uniq_per_sec, on="SEC_START", how="left")
        cov["DUPLICATES_10MS"] = cov["N_SAMPLES"] - cov["N_TICKS_10MS"]
        n_secs_200 = int((cov["N_SAMPLES"] >= 200).sum())
        dbg(f"[agg] segundos_com_200_amostras={n_secs_200}")
        if n_secs_200 > 0:
            dbg("[agg] exemplo segundos 200 amostras com diagnostico duplicatas:",
                cov[cov["N_SAMPLES"] >= 200][["SEC_START", "N_SAMPLES", "N_TICKS_10MS", "DUPLICATES_10MS"]].head(5))

        # 10. normalização opcional por grade de 10 ms antes da agregação de 1 s
        # razão: colapsar duplicatas exatas no mesmo tick de 10 ms reduz viés e uniformiza jitter
        # política: se houver qualquer segundo com N_SAMPLES > 100, aplica-se a normalização de 10 ms
        apply_collapse_10ms = bool((cov["N_SAMPLES"] > 100).any())
        if apply_collapse_10ms:
            dbg("[agg] normalizacao_10ms habilitada: colapsando duplicatas por TICK_10MS com mediana dos campos fisicos")
            # agrega por TICK_10MS mantendo metadados do primeiro registro do tick
            agg_map_tick = {f: "median" for f in float_fields}
            # metadados fora dos campos físicos são preservados via first
            for c in df.columns:
                if c not in agg_map_tick and c not in ["UTC_TIME", "SEC_START", "TICK_10MS"]:
                    agg_map_tick[c] = "first"

            df_tick = (
                df.groupby(["SEC_START", "TICK_10MS"], as_index=False)
                  .agg(agg_map_tick | {"UTC_TIME": "first"})
            )
            before = len(df)
            after = len(df_tick)
            dbg(f"[agg] efeito_normalizacao_10ms: n_amostras_antes={before} n_ticks_apos={after} "
                f"redução={(before - after)}")
            df = df_tick
        else:
            dbg("[agg] normalizacao_10ms desabilitada: amostragem por segundo já compatível com 100 Hz")

        # 11. mapeamento de agregação por segundo
        # razão: mediana para grandezas físicas e first para metadados
        agg_map_sec = {f: "median" for f in float_fields}
        for c in df.columns:
            if c not in agg_map_sec and c not in ["UTC_TIME", "SEC_START", "TICK_10MS"]:
                agg_map_sec[c] = "first"
        agg_map_sec["UTC_TIME"] = "first"

        # 12. agregação por 1 s
        # razão: consolidar 10 ms -> 1 s conforme especificação
        df1s = df.groupby("SEC_START", as_index=False).agg(agg_map_sec)
        n_secs_out = len(df1s)
        dbg(f"[agg] agregacao por segundo concluida: segundos_agregados={n_secs_out}")

        # 13. timestamp no centro da janela
        # razão: especificação define uso do centro da janela de 1 s
        df1s["UTC_TIME"] = df1s["SEC_START"] + pd.to_timedelta(499, unit="ms") + pd.to_timedelta(500, unit="us")
        bad_center = int((df1s["UTC_TIME"].dt.microsecond != 499500).sum())
        dbg(f"[agg] verificacao do centro temporal: microsegundos_divergentes={bad_center}")

        # 14. rederivacao de campos temporais auxiliares
        # razão: manter o mesmo esquema do dataset de origem
        df1s["UTC_TIME_YEAR"] = df1s["UTC_TIME"].dt.year
        df1s["UTC_TIME_MONTH"] = df1s["UTC_TIME"].dt.month
        df1s["UTC_TIME_DAY"] = df1s["UTC_TIME"].dt.day
        df1s["UTC_TIME_HOUR"] = df1s["UTC_TIME"].dt.hour
        df1s["UTC_TIME_MINUTE"] = df1s["UTC_TIME"].dt.minute
        df1s["UTC_TIME_SECOND"] = df1s["UTC_TIME"].dt.second
        df1s["UTC_TIME_MILLISECOND"] = 499
        df1s["HOUR"] = df1s["UTC_TIME"].dt.strftime("%H:%M:%S")
        df1s["TIME"] = (
            df1s["UTC_TIME_HOUR"] * 3600
            + df1s["UTC_TIME_MINUTE"] * 60
            + df1s["UTC_TIME_SECOND"]
        ).astype("int64")

        # 15. estatística de sanidade dos campos físicos após a mediana
        # razão: confirmar faixas plausíveis e presença de valores faltantes
        nan_post = {f: int(df1s[f].isna().sum()) for f in float_fields}
        dbg(f"[agg] NaN apos agregacao por mediana: {nan_post}")
        for f in float_fields:
            if f in df1s.columns and df1s[f].notna().any():
                vmin = float(pd.to_numeric(df1s[f], errors="coerce").min())
                vmax = float(pd.to_numeric(df1s[f], errors="coerce").max())
                dbg(f"[agg] faixa {f}: min={vmin:.4f} max={vmax:.4f}")

        # 16. sumário global do processo
        # razão: permitir auditoria numérica do efeito da transformação
        dbg(f"[agg] sumario: n_entradas={total_in}  n_segundos_entrada={n_secs_in}  n_segundos_saida={n_secs_out}")
        if n_secs_out != n_secs_in:
            dbg(
                f"[agg] atencao: n_segundos_saida({n_secs_out}) difere de n_segundos_entrada({n_secs_in}). "
                f"isso pode refletir normalizacao_10ms, segundos vazios ou gaps."
            )

        # 17. reconversao de campos numericos para string com 4 casas decimais
        # razão: alinhar com o formato do dataset original
        for f in float_fields:
            df1s[f] = df1s[f].map(lambda x: f"{x:.4f}" if pd.notnull(x) else None)

        # 18. limpeza de colunas auxiliares e conversao final para lista de dicionarios
        # razão: manter o esquema original e entregar a estrutura solicitada
        df1s = df1s.drop(columns=["SEC_START", "TICK_10MS"], errors="ignore")
        result = df1s.to_dict(orient="records")

        # 19. amostra final e confirmacao de contagem
        # razão: facilitar inspeção visual do resultado e confirmar cardinalidade
        dbg(f"[agg] saida final consolidada: n_registros={len(result)}")

        # 19–21. AUDITORIA CONSOLIDADA: gerar um ÚNICO TXT com 100% do rastreamento
        # razão: unificar verificação de faltantes, métricas de normalização 10 ms, cobertura e trilha completa por segundo (origem vs saída)
        try:
            # --- referências temporais básicas do INPUT ---
            t0_sec = t_min.floor("1s")
            t1_sec = t_max.floor("1s")
            expected_range_secs = int((t1_sec - t0_sec).total_seconds()) + 1 if pd.notna(t0_sec) and pd.notna(t1_sec) else 0

            # --- métricas de normalização 10 ms ---
            before_norm = locals().get("before", None)
            after_norm  = locals().get("after", None)
            norm_aplicada = bool(locals().get("apply_collapse_10ms", False))
            norm_reducao  = (before_norm - after_norm) if (before_norm is not None and after_norm is not None) else 0

            # --- estatísticas de ticks por segundo (origem) ---
            ticks_desc = {}
            if "N_TICKS_10MS" in cov.columns and len(cov) > 0:
                ticks_desc = {
                    "tick_min":      int(cov["N_TICKS_10MS"].min()),
                    "tick_mediana":  float(cov["N_TICKS_10MS"].median()),
                    "tick_p95":      float(cov["N_TICKS_10MS"].quantile(0.95)),
                    "tick_max":      int(cov["N_TICKS_10MS"].max()),
                }

            # --- saída agregada: métricas de cobertura, faltantes e duty por hora ---
            if len(result) > 0:
                out_times = pd.to_datetime([r.get("UTC_TIME") for r in result], utc=True, errors="coerce").floor("1s")
                t0_out = out_times.min()
                t1_out = out_times.max()
                full_out = pd.date_range(start=t0_out, end=t1_out, freq="1s", tz="UTC")
                observed_out = pd.DatetimeIndex(out_times.unique()).sort_values()
                missing_out  = full_out.difference(observed_out)
                total_missing_out = int(missing_out.size)
                expected_out_secs = int((t1_out - t0_out).total_seconds()) + 1 if pd.notna(t0_out) and pd.notna(t1_out) else 0
                coverage_out_pct  = (100.0 * (expected_out_secs - total_missing_out) / expected_out_secs) if expected_out_secs > 0 else 100.0

                # gaps contíguos na saída
                if total_missing_out > 0:
                    m = pd.Series(missing_out)
                    diffs   = m.diff().dt.total_seconds().fillna(0)
                    breaks  = diffs.ne(1).cumsum()
                    ranges_out = m.groupby(breaks).agg(["first","last","size"]).reset_index(drop=True)
                    ranges_out = ranges_out.rename(columns={"first":"START","last":"END","size":"LEN_S"})
                else:
                    ranges_out = pd.DataFrame(columns=["START","END","LEN_S"])

                # duty por hora na saída
                duty_por_hora = (
                    pd.Series(out_times)
                    .dt.floor("1h")
                    .value_counts()
                    .sort_index()
                    .rename_axis("HOUR_START")
                    .reset_index(name="N_SECONDS_COLETADOS")
                )
                if not duty_por_hora.empty:
                    duty_por_hora = duty_por_hora.assign(DUTY_PCT=lambda d: 100.0 * d["N_SECONDS_COLETADOS"] / 3600.0)
            else:
                t0_out = pd.NaT; t1_out = pd.NaT
                expected_out_secs = 0; total_missing_out = 0; coverage_out_pct = 0.0
                ranges_out = pd.DataFrame(columns=["START","END","LEN_S"])
                duty_por_hora = pd.DataFrame(columns=["HOUR_START","N_SECONDS_COLETADOS","DUTY_PCT"])

            # --- amostras de segundos com 200 amostras (origem) ---
            sample_200 = pd.DataFrame()
            if "N_SAMPLES" in cov.columns:
                sample_200 = cov[cov["N_SAMPLES"] >= 200][["SEC_START","N_SAMPLES","N_TICKS_10MS","DUPLICATES_10MS"]].head(20)

            # --- faixas físicas pós-mediana (usar df1s antes da formatação em string) ---
            faixas_fisicas = []
            for f in float_fields:
                if f in df1s.columns and df1s[f].notna().any():
                    vals = pd.to_numeric(df1s[f], errors="coerce")
                    faixas_fisicas.append((f, float(vals.min()), float(vals.max())))

            # --- trilha completa por segundo: ORIGEM vs SAÍDA (100%) ---
            full_idx = pd.date_range(start=t0_sec, end=t1_sec, freq="1s", tz="UTC")
            # origem (cov já contém N_SAMPLES; pode conter N_TICKS_10MS)
            cov_local = cov.copy()
            if "N_TICKS_10MS" not in cov_local.columns:
                _tmp_ticks = df.groupby("SEC_START")["UTC_TIME"].apply(lambda s: s.dt.floor("10ms").nunique())
                cov_local = cov_local.merge(_tmp_ticks.rename("N_TICKS_10MS"), on="SEC_START", how="left")
            # saída
            if len(result) > 0:
                observed_out_idx = pd.DatetimeIndex(out_times.unique()).sort_values()
            else:
                observed_out_idx = pd.DatetimeIndex([], tz="UTC")
            # compor a grade completa
            df_full = pd.DataFrame({"SEC_START": full_idx})
            df_full = df_full.merge(cov_local, on="SEC_START", how="left")
            df_full["HAS_INPUT"] = df_full["N_SAMPLES"].notna()
            df_full["N_SAMPLES"] = df_full["N_SAMPLES"].fillna(0).astype("int64")
            if "N_TICKS_10MS" in df_full.columns:
                df_full["N_TICKS_10MS"] = df_full["N_TICKS_10MS"].fillna(0).astype("int64")
            else:
                df_full["N_TICKS_10MS"] = 0
            df_full["HAS_OUTPUT"] = df_full["SEC_START"].isin(observed_out_idx)
            df_full = df_full.sort_values("SEC_START").reset_index(drop=True)

            # --- utilitário de seções ---
            def section(title: str) -> list[str]:
                bar = "=" * 100
                return [bar, f"### {title}", bar]

            lines: list[str] = []

            # seção: contexto geral do input
            lines += section("POEMAS AGG AUDIT — CONTEXTO GERAL (INPUT)")
            lines.append(f"input_n={total_in}")
            lines.append(f"input_time_min_utc={t_min}")
            lines.append(f"input_time_max_utc={t_max}")
            lines.append(f"input_secs_distintos={n_secs_in}")
            lines.append(f"input_secs_esperados_range={expected_range_secs}")
            lines.append(f"input_secs_faltantes_estimado={expected_range_secs - n_secs_in}")
            lines.append("")

            # seção: normalização 10 ms
            lines += section("NORMALIZAÇÃO 10ms (COLLAPSE)")
            lines.append(f"norm_10ms_aplicada={norm_aplicada}")
            lines.append(f"norm_input_registros_antes={before_norm}")
            lines.append(f"norm_ticks_apos={after_norm}")
            lines.append(f"norm_reducao={norm_reducao}")
            if ticks_desc:
                lines.append(f"ticks_10ms_stats_min={ticks_desc['tick_min']}")
                lines.append(f"ticks_10ms_stats_mediana={ticks_desc['tick_mediana']}")
                lines.append(f"ticks_10ms_stats_p95={ticks_desc['tick_p95']}")
                lines.append(f"ticks_10ms_stats_max={ticks_desc['tick_max']}")
            lines.append("")

            # seção: saída agregada e cobertura
            lines += section("SAÍDA AGREGADA 1s — COBERTURA E INTERVALOS")
            lines.append(f"output_secs={n_secs_out}")
            lines.append(f"output_time_min_utc={t0_out}")
            lines.append(f"output_time_max_utc={t1_out}")
            lines.append(f"output_secs_esperados_range={expected_out_secs}")
            lines.append(f"output_secs_faltantes={total_missing_out}")
            lines.append(f"output_coverage_pct={coverage_out_pct:.2f}")
            lines.append("")

            # seção: NaN entrada vs pós-mediana
            lines += section("NaN — ENTRADA vs PÓS-MEDIANA")
            lines.append("nan_conv_entrada=" + str(nan_conv))
            lines.append("nan_pos_mediana_saida=" + str(nan_post))
            lines.append("")

            # seção: faixas físicas pós-mediana
            lines += section("FAIXAS FÍSICAS PÓS-MEDIANA [min, max]")
            if faixas_fisicas:
                for f, mn, mx in faixas_fisicas:
                    lines.append(f"{f}: min={mn:.6f} max={mx:.6f}")
            else:
                lines.append("nenhum_campo_fisico_disponivel")
            lines.append("")

            # seção: segundos com 200 amostras (origem)
            lines += section("SEGUNDOS COM 200 AMOSTRAS — TOP 20 (ORIGEM)")
            if not sample_200.empty:
                lines.append(sample_200.to_string(index=False))
            else:
                lines.append("nenhum")
            lines.append("")

            # seção: gaps na saída (faixas)
            lines += section("GAPS NA SAÍDA — FAIXAS CONTÍGUAS")
            if not ranges_out.empty:
                lines.append(ranges_out.to_string(index=False))
                lines.append(f"gaps_saida_total_faixas={len(ranges_out)}")
                lines.append(f"gaps_saida_total_segundos={int(ranges_out['LEN_S'].sum())}")
                lines.append(f"gaps_saida_maior_faixa_s={int(ranges_out['LEN_S'].max())}")
            else:
                lines.append("nenhum")
            lines.append("")

            # seção: duty cycle por hora (saída)
            lines += section("DUTY CYCLE POR HORA — SAÍDA")
            if not duty_por_hora.empty:
                lines.append(duty_por_hora.to_string(index=False))
            else:
                lines.append("vazio")
            lines.append("")

            # seção: configuração e campos agregados
            lines += section("CONFIGURAÇÃO E CAMPOS AGRUPADOS")
            lines.append("campos_numericos_agrupados_por_mediana=" + ",".join(float_fields))
            lines.append("observacao=arquivo unificado para confrontar possíveis falhas de coleta (origem) vs lógica de agregação (código)")
            lines.append("")

            # seção: LISTAGEM COMPLETA (100%) — origem vs saída (um por linha)
            # motivo: confirmar de forma inequívoca a presença/ausência de cada segundo
            lines += section("LISTAGEM COMPLETA POR SEGUNDO — ORIGEM vs SAÍDA (100%)")
            # formata tabela completa
            full_table_str = df_full[["SEC_START","HAS_INPUT","N_SAMPLES","N_TICKS_10MS","HAS_OUTPUT"]].to_string(index=False)
            lines.append(full_table_str)
            lines.append("")

            # nome do arquivo com base no intervalo do input
            fname_tag_start = pd.to_datetime(t0_sec).strftime("%Y%m%dT%H%M%S") if pd.notna(t0_sec) else "NA"
            fname_tag_end   = pd.to_datetime(t1_sec).strftime("%Y%m%dT%H%M%S") if pd.notna(t1_sec) else "NA"
            audit_path = f"poemas_agg_audit_{fname_tag_start}_{fname_tag_end}.txt"

            with open(audit_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))

            dbg(f"[agg] audit_txt_unificado_gerado: {audit_path}")
        except Exception as e:
            dbg(f"[agg] erro_ao_gerar_audit_txt_unificado: {e}")


        return result
