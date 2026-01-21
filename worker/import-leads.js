import 'dotenv/config'
import { createClient } from '@supabase/supabase-js'
import XLSX from 'xlsx'

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const BATCH_SIZE = 500
const SLEEP_MS = 150

const sleep = ms => new Promise(r => setTimeout(r, ms))

async function run() {
  console.log('🚀 Worker started')

  // 1️⃣ Get pending import jobs
  const { data: jobs, error } = await supabase
    .from('import_jobs')
    .select('*')
    .eq('status', 'pending')
    .limit(1)

  if (error || !jobs.length) {
    console.log('✅ No pending jobs')
    return
  }

  const job = jobs[0]
  console.log('📂 Processing:', job.file_path)

  // 2️⃣ Download Excel
  const { data: file } = await supabase.storage
    .from('uploads')
    .download(job.file_path)

  const buffer = await file.arrayBuffer()
  const workbook = XLSX.read(buffer, { type: 'array' })
  const sheet = workbook.Sheets[workbook.SheetNames[0]]
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' })

  console.log(`📊 Rows found: ${rows.length}`)

  // 3️⃣ Insert in batches
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE)

    const { error } = await supabase.rpc('bulk_insert_leads', {
      json_data: batch
    })

    if (error) {
      console.error('❌ Batch failed', error)
      await supabase.from('import_jobs')
        .update({ status: 'failed', error: error.message })
        .eq('id', job.id)
      return
    }

    await sleep(SLEEP_MS)
  }

  // 4️⃣ Mark done
  await supabase.from('import_jobs')
    .update({ status: 'done' })
    .eq('id', job.id)

  console.log('✅ Import completed')
}

run().catch(console.error)
