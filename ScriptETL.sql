declare @datainicial date
declare @datafinal date
select @datainicial = min(dta_inclusao) from xxx.dbo.protocolo
select @datafinal = max(dta_inclusao) from xxx.dbo.protocolo
truncate table expurgo_edicoes
while (1=1 )
begin
if @datainicial > @datafinal break

insert into expurgo_edicoes select * from (
select 

vsd.id_secao_documento,
prot.id_protocolo,
min(dth_atualizacao)over(partition by vsd.id_secao_documento) as dth_criacao_documento,
dth_atualizacao as dth_assinatura_documento,
versao as quantidade_edicoes_documento ,
SUM(DATALENGTH(vsd.conteudo))over(partition by vsd.id_secao_documento) as tamanho_total_edicoes,
DATALENGTH(vsd.conteudo) as tamanho_ultima_edicao, 
SUM(DATALENGTH(vsd.conteudo))over(partition by vsd.id_secao_documento) - DATALENGTH(vsd.conteudo) as tamanho_diferenca,
SUM(DATALENGTH(vsd.conteudo))over(partition by vsd.id_secao_documento) / versao as media_bytes_edicoes,
sin_ultima

from xxx.dbo.versao_secao_documento vsd 

inner join xxx.dbo.secao_documento sd
    on sd.id_secao_documento = vsd.id_secao_documento
    AND sd.sin_principal = 'S'
inner join xxx.dbo.documento dc
    on dc.id_documento = sd.id_documento
inner join xxx.dbo.protocolo prot
    on prot.id_protocolo = dc.id_documento
    and prot.sta_protocolo = 'G'
    where year(prot.dta_inclusao) = year(@datainicial) and  month(prot.dta_inclusao) = month(@datainicial)
    --order by 5 desc
    --and sin_ultima = 'S' 
) z1 where z1.sin_ultima = 'S'
declare @text varchar(50)
set @text =@datainicial
RAISERROR ( @text , 0, 1) WITH NOWAIT
select @datainicial= DATEADD(month,1,@datainicial)
end
