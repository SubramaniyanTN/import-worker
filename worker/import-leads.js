import 'dotenv/config'
import { createClient } from '@supabase/supabase-js'
import XLSX from 'xlsx'

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const BATCH_SIZE = 500
const SLEEP_MS = 100
const POLL_INTERVAL = 30_000 // check every 30 seconds

const sleep = ms => new Promise(r => setTimeout(r, ms))

async function processJob(job) {
  console.log('📂 Processing:', job.file_path)

  const { data: file, error: downloadError } = await supabase.storage
    .from('uploads')
    .download(job.file_path)

  if (downloadError || !file) {
    throw new Error('File download failed')
  }

  const buffer = Buffer.from(await file.arrayBuffer())
  const workbook = XLSX.read(buffer)
  const sheet = workbook.Sheets[workbook.SheetNames[0]]
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' })

  console.log(`📊 Rows found: ${rows.length}`)

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE)

    const { error } = await supabase.rpc('bulk_insert_leads', {
      json_data: batch
    })

    if (error) throw error
    await sleep(SLEEP_MS)
  }

  await supabase
    .from('import_jobs')
    .update({ status: 'done' })
    .eq('id', job.id)

  console.log('✅ Job completed:', job.id)
}

async function workerLoop() {
  console.log('🚄 Import worker running')

  while (true) {
    try {
      const { data: jobs, error } = await supabase
        .from('import_jobs')
        .select('*')
        .eq('status', 'pending')
        .order('created_at', { ascending: true })
        .limit(1)

      if (error) throw error

      if (!jobs.length) {
        console.log('⏳ No pending jobs')
        await sleep(POLL_INTERVAL)
        continue
      }

      const job = jobs[0]

      await supabase
        .from('import_jobs')
        .update({ status: 'processing' })
        .eq('id', job.id)

      await processJob(job)

    } catch (err) {
      console.error('❌ Worker error:', err.message)
      await sleep(10_000)
    }
  }
}

workerLoop()
